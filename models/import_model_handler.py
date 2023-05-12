# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

import logging
import xmlrpc.client
import traceback
import time

_logger = logging.getLogger("colpari_odoo_import")
_logLevelMap = {
	'3_debug' 	: logging.INFO, #logging.DEBUG,
	'2_info' 	: logging.INFO,
	'1_warning'	: logging.WARNING,
	'0_error' 	: logging.ERROR,
}

LOCAL_SCHEMA_OVERRIDE = {
	'account.move.line' : {
		'account_id' : { 'required' : True } # required for create
	},
	# normally product.template manages/creates the product.product instances during create()
	# we make both sides non-required to be able to connect them after create()
	'product.template' : {
		'product_variant_ids' : { 'required' : False }
	},
	'product.product' : {
		'product_variant_id' :  { 'required' : False },
		'product_variant_ids':  { 'required' : False }
	}
}

CREATE_CONTEXTS = {
	'product.template' : {
		'create_product_product' : True # inhibit auto-creating product.product from product.template
	}
}


def SELECT_KEY(key):
	return lambda x : x[key]

def TO_DICT(keyName, iterable):
	result = {}
	for x in iterable:
		#TODO: assert keys unique?
		key = x[keyName]
		if key in result:
			raise Exception("Key '{}' is already in dictionary".format(key))
		result[key] = x
	return result



# (m1, m2, m3, m4, m5) = env['account.move'].browse([3036, 2731, 2451, 2189, 1995])
# for f in sorted(m1.fields_get().keys()):
#   print("{} | {} | {} | {} | {} | {}".format(f, m1[f], m2[f], m3[f], m4[f], m5[f]))

class ImportException(Exception):
	def __init__(self, message, **kwargs):
		self.kwargs = kwargs
		super(ImportException, self).__init__(message)

class ImportModelHandler():
	def __init__(self, parentContext, modelName):
		self.CTX 			= parentContext
		self.modelName		= modelName
		self.env 			= parentContext.env
		self.importRun 		= parentContext.importRun
		self.log 			= parentContext.importRun._log
		self.importConfig 	= parentContext.importConfig
		self.remoteOdoo		= parentContext.remoteOdoo

		self.__sharedDependencyWorkList = parentContext._sharedDependencyWorkList

		self.modelConfig 	= self.importConfig.getModelConfig(modelName)
		self.fieldConfigs	= {}
		self.fieldNamesL2R 	= {} # field name mapping local -> remote for colpari.odoo_import_fieldconfigs with remote_field_name
		self.fieldNamesR2L 	= {} # field name mapping remote -> local for colpari.odoo_import_fieldconfigs with remote_field_name

		if self.modelConfig:
			for fieldConfig in self.modelConfig.field_configs:
				fieldName = fieldConfig.import_field.name
				self.fieldConfigs[fieldName] = fieldConfig
				remoteFieldName = fieldConfig.remote_field_name
				if remoteFieldName:
					if remoteFieldName in self.fieldNamesR2L:
						raise ValidationError("Two fields of {} divert to the same remote field name '{}' : [{}, {}]".format(
							modelName, remoteFieldName, fieldName, self.fieldNamesR2L[fieldName]
						))
					self.fieldNamesR2L[remoteFieldName] = fieldName
					self.fieldNamesL2R[fieldName] = remoteFieldName


			self.importStrategy = self.modelConfig.model_import_strategy
			self.matchingStrategy = self.modelConfig.matching_strategy
		else:
			self.importStrategy = 'match'
			self.matchingStrategy = 'odooName'

		self.remoteFields 		= {} 	# lazy fieldName -> properties
		self.localFields 		= {} 	# lazy fieldName -> properties
		self.remoteIdFields 	= None 	# [fn, fn, fn...]
		self.fieldsToImport		= None 	# lazy set((fn, f))
		self.relFieldsToImport	= None 	# lazy set((fn, f))
		self.o2mFieldsWeImportTargetsOf = None # lazy set((fn, f))

		self._fieldProperties	= { # lazy index of local field properties by propertyName
			# propertyName : {
			# 	'names'		: set(), # all field names where propertyName is True
			# 	'dicts'		: { fieldName : fieldDef }, # all field definitions where propertyName is True
			# 	'values' 	: { fieldName : propertyValue } # property values of all fields where propertyName is True
			# }
		}

		# FIXME prefix these internal fields with __
		self.remoteData = {
			# id : { field : value, ... }
		}

		self.keyMaterial	= None 	# { remoteId : { k :v, k :v... }}
		self.idMap			= {} 	# { remoteId : localId }
		self.idMapReverseCheck	= {}# { localdId : remoteId }
		self.toCreate		= {} 	# { remoteId : { k :v, k :v... } }
		self.toUpdate		= {} 	# { remoteId : { k :v, k :v... } }

		# keeps track of all remote key combinations to assure they are unique in the first place
		self.remoteKeyUniquenessCheck = {} # {  kv1 : { kv2 : { k3v .. : set(remoteId) }}}

	def __repr__(self):
		return "<{} handler>".format(self.modelName)

	def idMapAdd(self, localId, remoteId):
		''' assure self.idMap has not duplicate value and we also never try to add the same key twice '''
		if localId in self.idMapReverseCheck:
			raise Exception("{} : local id {} was mapped to multiple remote ids : [{}, {}]".format(
				self.modelName, localId, remoteId, self.idMapReverseCheck[localId]
			))

		if remoteId in self.idMap:
			raise Exception("{} : remote id {} was mapped to multiple local ids : [{}, {}]".format(
				self.modelName, remoteId, localId, self.idMap[remoteId]
			))

		self.idMap[remoteId] = localId
		self.idMapReverseCheck[localId] = remoteId

	def checkConfig(self):
		''' NOTE: must be called right after all ImportModelHandler instances are created '''
		if not self.modelConfig:
			return True

		if self.matchingStrategy == 'explicitKeys' and not self.modelConfig.getConfiguredKeyFields():
			raise ImportException("Matching strategy for {} is 'explicitKeys' but no key fields are configured".format(self.modelName))

		if self.hasImportStrategy('ignore', 'match'):
			# check only types we read/write data of
			return True

		# if self.modelConfig.model_remote_domain:
		# 	_logger.info("{} remote domain is '{}'".format(
		# 		self.modelName, self.modelConfig.model_remote_domain
		# ))

		# read remote model
		remoteFields = self.getRemoteFields()
		localFields = self.getLocalFields()
		#msg = "checking {}/{} local/remote fields".format(len(localFields), len(remoteFields))
		#self.log('3_debug', msg, modelName=self.modelName)

		# determine which fields to import
		fieldsToImport = self.getFieldsToImport()

		# check if we should be able to provide all locally required fields
		#TODO?: warn if there are explicit key fields which are not to be imported (done in _getRemoteIdFields() currently)
		for fieldName, field in localFields.items():
			required = field.get('required')
			relatedTypeName = field.get('relation')
			ignored = self.getFieldImportStrategy(fieldName) == 'ignore'
			isPartOfKey = self.getFieldImportStrategy(fieldName) == 'key'
			#_logger.info("checking field {} on {}; req={}, rel={}, ign={}".format(fieldName, self.modelName, required, relatedTypeName, ignored))
			if ignored:
				# NOTE: this mutates self.fieldsToImport
				# 	thus, this method is part of the is-proper-set-up path for this class
				# 	and must be called right after all ImportModelHandler instances are created
				# FIXME:? this cannot be done in __init__ since self.CTX.getHandler(relatedTypeName) might call __init__ for other models and loops may occur
				fieldsToImport.pop(fieldName, 0)

			if required:
				if ignored:
					raise ImportException("Field {}.{} is required but ignored in import configuration".format(self.modelName, fieldName))

				if fieldName not in fieldsToImport:
					fc = self.getFieldConfig(fieldName)
					if not (fc and fc.mapsToDefaultValue()): # do we have a default?
						raise ImportException(
							"Field {}.{} is required but not to be imported (not found on remote side?) and there is no default value configured".format(
							self.modelName, fieldName
						))

				if relatedTypeName and self.CTX.getHandler(relatedTypeName).importStrategy == 'ignore':
					raise ImportException("Field {}.{} is required but it contains type {} which is explicitly ignored in import configuration".format(
						self.modelName, fieldName, relatedTypeName
					))

			if isPartOfKey and relatedTypeName and field["type"] != 'many2one':
					raise ImportException("Field X2many field {}.{} is not supported as key field: {}".format(
						self.modelName, fieldName, field
					))

			#FIXME: since we don't write o2m-fields for relying on their m2o-counterpart, assure that each counterpart will be imported

		if not fieldsToImport:
			raise ImportException(
				"Empty import field list for {}".format(self.modelName), modelName=self.modelName
			)

		# unimportedFields = set(localFields.keys()) - fieldsToImport.keys()
		# if unimportedFields:
		# 	self.log('3_debug', "unimported fields : {}".format(unimportedFields), modelName=self.modelName)

		return True


	def hasImportStrategy(self, *strategies):
		return self.importStrategy in strategies

	def getFieldConfig(self, fieldName):
		return self.fieldConfigs.get(fieldName)

	def getFieldImportStrategy(self, fieldName):
		if self.hasImportStrategy('match', 'ignore'):
			return 'ignore'
		fc = self.getFieldConfig(fieldName)
		# unconfigured fields of imported types default to being imported
		return fc and fc.field_import_strategy or 'import'


	def getLocalFields(self):
		if not self.localFields:
			# make a copy of the field info because we inject data into it
			self.localFields = {}
			localOverrides = LOCAL_SCHEMA_OVERRIDE.get(self.modelName)
			for fieldName, field in self.env[self.modelName].fields_get().items():
				_field = dict(field)
				fieldOverride = localOverrides and localOverrides.get(fieldName)
				if fieldOverride:
					_field.update(fieldOverride)
				self.localFields[fieldName] = _field

		return self.localFields

	def getRemoteFields(self): # FIXME: check all invocations if the are aware of remote diversions
		if not self.remoteFields:
			self.remoteFields = self.CTX.remoteOdoo.getFieldsOfModel(self.modelName)
		return self.remoteFields

	def shouldFollowDependency(self, fieldName):
		if not self.modelConfig:
			raise Exception("ASSERT: shouldFollowDependency() must not be called with unconfigured type {}".format(self.modelName))

		if not self.hasImportStrategy('import', 'bulk'):
			raise Exception(
				"ASSERT: shouldFollowDependency() must not be called with non-imported type {} (strategy={})".format(
					self.modelName, self.importStrategy
			))

		localField = self.getLocalFields()[fieldName] # raise KeyError for unknown fields

		relatedTypeName = localField.get('relation')
		if not relatedTypeName:
			raise Exception(
				"ASSERT: shouldFollowDependency() must not be called with non-relation field {}.{}".format(
					modelName, fieldName
			))

		relatedType = self.CTX.getHandler(relatedTypeName)
		required = localField.get('required')

		if relatedType.importStrategy == 'ignore':
			if required:
				raise ImportException("Type {} is required as dependency for {}.{} but ignored".format(
					relatedType.modelName, self.modelName, fieldName,
					modelName=self.modelName, fieldName=fieldName, dependencyType=relatedType.modelName
				))
			return False

		if localField.get('type') == 'one2many':
			# always follow one2many TODO: sure?/configurable?
			return relatedType

		if not required:
			if self.importConfig.only_required_dependencies or self.modelConfig.only_required_dependencies:
				return False

		return relatedType


	def getFieldsToImport(self):
		if self.fieldsToImport == None:
			if not self.hasImportStrategy('import', 'bulk'):
				raise Exception("Code path error. getFieldsToImport() called for strategy {}".format(self.importStrategy))

			self.fieldsToImport = {}
			localFields = self.getLocalFields()
			remoteFields = self.getRemoteFields()
			followedNotified = set()
			for fn, f in localFields.items():
				if fn not in remoteFields:
					continue
				# if f.get('related'):
				# 	continue
				relatedTypeName = f.get('relation')
				if relatedTypeName:
					handler = self.shouldFollowDependency(fn)
					if not handler:
						continue
					elif handler not in followedNotified: # and not handler.modelConfig
						# log message if relatedTypeName is not configured (we implicitly follow it) once
						self.log(
							'3_debug', "following dependency {}::{} -> {}".format(self.modelName, fn, relatedTypeName),
							modelName = self.modelName, fieldName = fn, dependencyType = relatedTypeName
						)
						followedNotified.add(handler) # log once only

				self.fieldsToImport[fn] = f

		return self.fieldsToImport

	def getRelFieldsToImport(self):
		if self.relFieldsToImport == None:
			if not self.hasImportStrategy('import', 'bulk'):
				raise Exception("Code path error. getRelFieldsToImport() called for strategy {}".format(strategy))

			self.relFieldsToImport = {
				fn : f for fn, f in self.getFieldsToImport().items() if f.get('relation')
			}

		return self.relFieldsToImport

	def __getO2MFieldsWeImportTargetsOf(self):
		if self.o2mFieldsWeImportTargetsOf == None:
			self.o2mFieldsWeImportTargetsOf = {}
			for fn, field in self.getRelFieldsToImport().items():
				if (field.get('type') == 'one2many'
				and self.CTX.getHandler(field['relation']).hasImportStrategy('import', 'bulk')):
					self.o2mFieldsWeImportTargetsOf[fn] = field

		return self.o2mFieldsWeImportTargetsOf

	def _getPropertiesEntry(self, propertyName):
		entry = self._fieldProperties.setdefault(propertyName, {})
		if not entry:
			# initialize if empty
			names  = entry['names'] = set()
			dicts  = entry['dicts'] = {}
			values = entry['values'] = {}
			notNames  = entry['!names'] = set()
			notDicts  = entry['!dicts'] = {}
			notValues = entry['!values'] = {}
			for fn, f in self.getLocalFields().items():
				propVal = f.get(propertyName)
				if propVal:
					names.add(fn)
					dicts[fn] = f
					values[fn] = propVal
				else:
					notNames.add(fn)
					notDicts[fn] = f
					notValues[fn] = propVal

			#FIXME: freeze data
		return entry

	def fieldsWhere(self, propertyName):
		return self._getPropertiesEntry(propertyName)['dicts']

	def fieldNamesWhere(self, propertyName):
		return self._getPropertiesEntry(propertyName)['names']

	def fieldNamesWhereNot(self, propertyName):
		return self._getPropertiesEntry(propertyName)['!names']

	def fieldProperties(self, propertyName):
		return self._getPropertiesEntry(propertyName)['values']

	def __getUnresolvedDependenciesOfRecords(self, records, requiredOnly, relKeysOnly):

		dependenciesPerTargetType = {}
		dependenciesPerId = {}

		relFieldNames = self.getRelFieldsToImport().keys()

		if requiredOnly:
			relFieldNames = set(relFieldNames) & self.fieldNamesWhere('required')

		if relKeysOnly:
			relFieldNames = [ fn for fn in relFieldNames if self.getFieldImportStrategy(fn) == 'key' ]

		for relationFieldName in relFieldNames:
			relatedType = self.shouldFollowDependency(relationFieldName)
			if not relatedType:
				raise ValidationError(
					"Cannot find related type handler for field {}.{}. This should not happen, since relFieldsToImport is already filtered by shouldFollowDependency()??".format(
					self.modelName, self.getLocalFields().get(relationFieldName) or relationFieldName
				))

			dependencyIds = set()
			for remoteId, remoteRecord in records.items():
				relationFieldValue = remoteRecord.get(relationFieldName)
				if relationFieldValue:
					relationIdsNotFound = list(filter(lambda x: x not in relatedType.idMap, relationFieldValue))
					if relationIdsNotFound:
						dependencyIds.update(relationIdsNotFound)
						dependenciesPerId.setdefault(remoteId, {}).setdefault(relatedType, set()).update(relationIdsNotFound)

			if dependencyIds:
				#_logger.info("{}.{} depends on {}::{}".format(self.modelName, relationFieldName, relatedType, dependencyIds))
				dependenciesPerTargetType.setdefault(relatedType, set()).update(dependencyIds)

		return (dependenciesPerTargetType, dependenciesPerId)

	def __mapRecords(self, records, requiredDependenciesOnly):
		''' - copy records and map remote ids in all relation fields to local ids
			- if requiredDependenciesOnly remove all non-required relation fields from result
			- remove all o2m-fields where we also import the corresponding m2o field anyway
			- apply configured field value mappings
			- convert many2One fields to single values since we handle them as lists internally
		'''
		relFields = self.getRelFieldsToImport()
		result = {}
		# copy all records
		for remoteId, remoteRecord in records.items():
			result[remoteId] = dict(remoteRecord) # copy

		o2mFieldsWeImportTargetsOf = self.__getO2MFieldsWeImportTargetsOf()

		for fieldName, field in self.getFieldsToImport().items():
			if fieldName in relFields:
			# relation field
				removeThisField = (
					(requiredDependenciesOnly and not field.get('required'))
					or
					(fieldName in o2mFieldsWeImportTargetsOf) #FIXME: this should maybe already be handled in getRelFieldsToImport() to save reading and processing it
				)
				if removeThisField:
					# this is to be removed because of requiredDependenciesOnly == True or o2m-field handling
					for remoteRecord in result.values():
						remoteRecord.pop(fieldName, 0)
					continue
			# relation field to map
				relatedType = self.shouldFollowDependency(fieldName) # should never be False here
				for remoteRecord in result.values():
					relationFieldValue = remoteRecord.get(fieldName)
					if relationFieldValue:
						try:
							mappedIds = list(map(relatedType.idMap.__getitem__, relationFieldValue))
						except KeyError:
							raise Exception("Missing mapped value for {} of field {} in idMap of {} record (out of {}) is: {}\nidMap: {}".format(
								relationFieldValue, fieldName, self.modelName, len(result), remoteRecord, relatedType.idMap
							))
						remoteRecord[fieldName] = (
							mappedIds[0] # convert many2One fields to single values since we handle them as lists internally
								if field.get('type') == 'many2one' else
							mappedIds
						)


			else:
			# normal field to map
				fc = self.getFieldConfig(fieldName)
				defaultValue = fc and fc.mapsToDefaultValue()
				decimalPrecision = fc and fc.decimal_precision
				if defaultValue:
					# apply default value for all records
					for remoteRecord in result.values():
						remoteRecord[fieldName] = defaultValue
				if decimalPrecision:
					# round values for all records
					for remoteRecord in result.values():
						value = remoteRecord.get(fieldName)
						if value:
							remoteRecord[fieldName] = round(value, decimalPrecision)

		if len(records) != len(result): # sanity check
			raise ValidationError("I did not copy&return all records for {}? ({}/{})\nrecords: {}\n\nresult: {}".format(
				self.modelName, len(records), len(result), records, result
			))

		return result

	def _readRemoteDataAndCollectDepenencies(self, remoteIds, dependencyIdsToResolve, relKeysOnly = False):
		# fetch remote data
		if not remoteIds:
			raise Exception("Empty id set")
		remoteData = self.readRemoteData(remoteIds)

		#TODO: are there cases where we could go with requiredOnly = True and thus save time?
		(dependenciesPerTargetType, dependenciesPerId) = self.__getUnresolvedDependenciesOfRecords(
			remoteData, requiredOnly = False, relKeysOnly = relKeysOnly
		)

		for _type, ids in dependenciesPerTargetType.items():
			dependencyIdsToResolve.setdefault(_type, set()).update(ids)

		return remoteData

	def _getRemoteIdFields(self):
		''' determines the required fields for identifying the remote models  '''
		if self.remoteIdFields == None:
			if self.matchingStrategy.startswith('odooName'):
				self.remoteIdFields = { 'display_name' }
				if 'name' in self.getRemoteFields():
					self.remoteIdFields.add('name')

			elif self.matchingStrategy == 'explicitKeys':
				self.remoteIdFields = self.modelConfig.getConfiguredKeyFieldNames()
				if not self.remoteIdFields:
					raise UserError("Model type {} has matching strategy 'explicitKey' but no key fields are configured")

				unimportedKeyFields = self.remoteIdFields - self.getFieldsToImport().keys()
				if unimportedKeyFields:
					raise ImportException("The fields {} of type {} are configured as key but not to be imported".format(
						unimportedKeyFields, self.modelName
					))
			else:
				raise Exception("Model matching strategy '{}' is not supported for {}".format(self.matchingStrategy, self.modelName))

		return self.remoteIdFields

	def status(self):
		checkSum = set(self.idMap.keys()) ^ (self.keyMaterial and self.keyMaterial.keys() or set()) ^ self.toUpdate.keys() ^ self.toCreate.keys()
		return "{} {} \t{} \t: \t{} keys, \t{} \tmapped, \t{} \tto update, \t{} \tto create \t(checkSum={})".format(
			#TODO: it would be really neat to shorten model names the java way: accouńt.fiscal.position -> a.f.position
			self.modelName.ljust(25), self.importStrategy, self.matchingStrategy,
			len(self.keyMaterial or []), len(self.idMap), len(self.toUpdate), len(self.toCreate), len(checkSum)
		)

	def isFinished(self):
		return not (self.toCreate or self.toUpdate)

	def hasContent(self):
		return self.toCreate or self.toUpdate or self.keyMaterial or self.idMap

	def hasWork(self):
		return self.toCreate or self.toUpdate or self.keyMaterial

	def tryCreate(self, dependencyIdsToResolve):

		if not self.hasImportStrategy('import', 'bulk'):
			raise Exception("tryCreate({}) should not be called for importStrategy '{}'".format(self.modelName, self.importStrategy))

		if self.importStrategy == 'import':
			self.resolveReadAndSchedule(dependencyIdsToResolve)

		if not self.toCreate:
			# nothing queued for create. return success but not progess
			return (True, False) # (finished, progess)

		# fetch unsatisfied dependencies of self.toCreate
		(dependenciesPerTargetType, dependenciesPerId) = self.__getUnresolvedDependenciesOfRecords(
			self.toCreate, requiredOnly = True, relKeysOnly = False
		)

		# check which object dependencies are fully resolved now and can be created
		_toCreate = {}
		_toTryLater = {}

		for remoteId, remoteRecord in self.toCreate.items():
			if remoteId in dependenciesPerId:
				# has unresolved dependencies
				_toTryLater[remoteId] = remoteRecord
			else:
				_toCreate[remoteId] = remoteRecord

		if not _toCreate:
			# unable to write anything
			_logger.info("{} has unresolved dependencies to {} objects of types {}".format(
				self.modelName, sum(map(len, dependenciesPerTargetType.values())), list(dependenciesPerTargetType.keys())
			))
			return (False, False) # (finished, progess)

		# we could at least write some records. do so if it is not bulk data
		if self.importStrategy == 'bulk' and _toTryLater:
			_logger.info("{} delaying creation of {}/{} bulk records".format(
				self.modelName, len(_toCreate), len(self.toCreate)
			))
			return (False, False) # (finished, progess)

		_logger.info("{} creating {}/{} records".format(
			self.modelName, len(_toCreate), len(self.toCreate)
		))

		objectSetMapped = self.__mapRecords(_toCreate, requiredDependenciesOnly = True)
		recordsToCreate = list(objectSetMapped.values())

		# __mapRecords(requiredDependenciesOnly = True) removed all non-required dependency fields (if any)...
		# ... put them into the 'update' stage (self.toUpdate)
		optionalRelatedFields = (
			(self.fieldNamesWhereNot('required') & self.getRelFieldsToImport().keys())
		  - self.__getO2MFieldsWeImportTargetsOf().keys()
		 )

		updatesScheduled = 0
		if optionalRelatedFields:
			for remoteId, unMappedRecord in _toCreate.items():
				updateData = {}

				for fn in optionalRelatedFields:
					#NOTE: using copy of UNmapped value for later update (because it will map again)
					unmappedValue = unMappedRecord[fn]
					if unmappedValue:
						updateData[fn] = list(unmappedValue) # explicit copy

				if not updateData:
					# all relfields are empty
					pass
				else:
					if remoteId in self.toUpdate: # sanity check
						raise ValidationError("{} remote id {} should not yet be present in self.toUpdate".format(
							self.modelName, remoteId
						))
					self.toUpdate[remoteId] = updateData
					updatesScheduled+=1

		try:
			createResult = self.env[self.modelName].with_context(
				**CREATE_CONTEXTS.get(self.modelName, {})
			).create(recordsToCreate)
		except Exception as e:
			_logger.info("create FAILED:\n{}".format(recordsToCreate[:300]))
			raise e

		if len(recordsToCreate) != len(createResult): # sanity check
			raise ValidationError("Got {} created records when trying to create {}".format(
				len(createResult), len(recordsToCreate)
			))
		# add ids of newly created records to idMap
		# NOTE: this assumes model.create() returns the created records in the same order as the supplied data.
		#		which proves to be true for odoo 14 and 15 at least :-}
		# NOTE: this also depends on the fact that we can leave the remoteId in the 'id'-field when calling create to
		# 		have it for updating self.idMap below
		# FIXME: unit test
		i = 0
		# _logger.info("{} 2 create: {}".format(self.modelName, recordsToCreate))
		# _logger.info("{} created : {}".format(self.modelName, createResult))
		for created in createResult:
			self.idMapAdd(localId = created['id'], remoteId = recordsToCreate[i]['id'])
			i+=1

		self.env[self.modelName].flush()

		_logger.info("{} CREATED {}/{} records ({} updates scheduled)".format(
			self.modelName, len(recordsToCreate), len(self.toCreate), updatesScheduled
		))

		self.toCreate = _toTryLater
		return (len(_toTryLater) == 0, True) # (success, progess)

	def __wouldBeChangedBy(self, localObject, remoteValues):
		''' returns True if remoteValues contains any values not present in localObject '''
		wouldChange = False
		for fn, value in remoteValues.items():
			field = self.getLocalFields()[fn]
			isRelation = field.get('relation')
			isMany2One = field.get('type') == 'many2one'
			localValue = localObject[fn]
			if isMany2One:
				localValue = localValue.id
			elif isRelation:
				localValue = localValue.ids

			if isRelation and not isMany2One:
				if (set(localValue) ^ set(value)):
					wouldChange = True
			else:
				if localValue != value:
					wouldChange = True

		return wouldChange

	def __logUpdateObjectFailed(self, localId, localObject, remoteId, remoteRecord, dataToUpdate, e):
		currentObjContent = str(
			{ fn : localObject[fn] for fn in localObject._fields.keys() }
		)
		msg = "update for {}.{} (remoteId={}) with FAILED: {}\ncurrent content:{}\nupdate content :{}\nremote content :{}".format(
				self.modelName, localId, remoteId, e, currentObjContent, dataToUpdate, remoteRecord
			)

		# debug unbalanced account.moves when account.move.line update fails
		if localObject._name == 'account.move.line':
			for line in localObject.move_id.line_ids:
				msg+="\n{} cred={}, deb={}, bal={}".format(line, line.credit, line.debit, line.balance)
			csum = sum(localObject.move_id.line_ids.mapped('credit'))
			dsum = sum(localObject.move_id.line_ids.mapped('debit'))
			bsum = sum(localObject.move_id.line_ids.mapped('balance'))
			msg+="\ncsum={}, dsum={}, bsum={}, +/- c/d = {}".format(csum, dsum, bsum, csum-dsum)

		return msg

	def tryUpdate(self):
		if not self.toUpdate:
			return (True, False)  # (finished, progess)

		(dependenciesPerTargetType, dependenciesPerId) = self.__getUnresolvedDependenciesOfRecords(
			self.toUpdate, requiredOnly = False, relKeysOnly = False
		)

		# any unresolved dependencies left?
		if dependenciesPerTargetType:
			# we cannot write
			raise ValidationError("Unresolved dependencies in update stage for {}?\n{}".format(
				self.modelName, dependenciesPerTargetType
			))
			#return False

		# nope - let's go!
		_logger.info("{} updating {} records".format(
			self.modelName, len(self.toUpdate)
		))

		objectSetMapped = self.__mapRecords(self.toUpdate, requiredDependenciesOnly = False)
		# map ids of objectSetMapped
		mappedIds = list(map(self.idMap.__getitem__, objectSetMapped.keys()))
		# locally fetch objects with mapped ids
		localObjects = TO_DICT('id',
			self.env[self.modelName].browse(mappedIds)
		)
		# check size
		if len(localObjects) != len(self.toUpdate): # sanity check
			raise ValidationError("Got {} local objects when browsing for {} ids".format(len(localObjects), len(self.toUpdate)))

		# update local objects one by one
		startTime = time.time()
		processed = 0
		failed = 0
		for remoteId, dataToUpdate in objectSetMapped.items():
			localId = self.idMap[remoteId]
			localObject = localObjects[localId]
			if self.modelName == 'res.partner.bank':
				_logger.info("{} updating {} with {}".format(self.modelName, localObject, dataToUpdate))
			try:
				#FIXME: evaluate if we want this
				# #if self.__wouldBeChangedBy(localObject, dataToUpdate):
				# 	# only call update if we really have different values
				# 	#localObject.update(dataToUpdate)
				localObject.write(dataToUpdate)

				# since we update one object at a time and it takes ages, log a message every 10 seconds
				processed += 1
				now = time.time()
				if (now - startTime) > 10:
					startTime = now
					_logger.info("{} update wrote {} of {} records".format(
						self.modelName, processed, len(objectSetMapped)
					))

			except Exception as e:
				failed +=1
				msg = self.__logUpdateObjectFailed(localId, localObject, remoteId, self.toUpdate[remoteId], dataToUpdate, e)
				if failed == 30:
					raise ImportException(msg, modelName = self.modelName)
				else:
					_logger.error(msg)

		recordsWritten = len(self.toUpdate)
		self.env[self.modelName].flush()
		_logger.info("{} UPDATED {} records".format(self.modelName, recordsWritten))
		self.toUpdate.clear()
		return (True, True) # (finished, progess)

	def resolveReadAndSchedule(self, dependencyIdsToResolve):
		''' splits the provided id set in 3 distincs sets:
				1. ids we know the local id for
				2. ids not mappable to a local id
				3. ids we don't yet know if they belong into set 1 or set 2

				keyMaterial  -> resolve
		'''
		(resolvedIds, unresolvedIds, pendingIds) = self.__resolve() # raises if strategy != import or match

		# importStrategy is match and all are matched -> done
		# OR
		# importStrategy is import and we need to schedule:

		if self.importStrategy == 'import':
			idsToImport = set()
			#idsToImport = set(pendingIds) # we need to read these to resolve their dependencies

			if self.modelConfig.do_create:
				idsToImport.update(unresolvedIds)

			if self.modelConfig.do_update:
				idsToImport.update(resolvedIds)

			if idsToImport:
				#_logger.info("{} {} ids to import".format(self.modelName, len(idsToImport)))
				# sanity check. none of idsToImport should be on the final worklists yet
				duplicates = idsToImport & (self.toCreate.keys() | self.toUpdate.keys())
				if duplicates:
					raise Exception(
						"Inconsistent worklist state for {}:\nidsToImport : {}\n2create    : {}\n2update    :{}\npending    :{}\nidMap      :{}".format(
							self.modelName, idsToImport, self.toCreate.keys(), self.toUpdate.keys(), self.keyMaterial.keys(), self.idMap.keys()
					))
				#_logger.info("{} reading data for {} ids".format(self.modelName, len(idsToImport)))
				# read ids to import and put into toCreate/toUpdate
				remoteData = self._readRemoteDataAndCollectDepenencies(idsToImport, dependencyIdsToResolve)
				for remoteId, remoteRecord in remoteData.items():
					if remoteId in unresolvedIds:
						self.toCreate[remoteId] = remoteRecord
					elif remoteId in resolvedIds:
						self.toUpdate[remoteId] = remoteRecord
					elif remoteId in self.keyMaterial:
						pass
					else: # sanity check. could only hit in case _readRemoteDataAndCollectDepenencies() returns unrequested data
						raise Exception(
							"ID {} for {} should be either in resolved, unresolved or pending:\nrequested: {}\nresolved: {}\nunresolved: {}\npending: {}".format(
								remoteId, self.modelName, idsToImport, resolvedIds, unresolvedIds, pendingIds
							)
						)

			if pendingIds:
				# we need to fetch the data for the pending ids too, but only collect key-fied dependencies needed for resolving
				# FIXME: we should forbid having relation key fields which point to 'bulk' types.
				# 	because, if configured, this causes dangling bulk data to be put on the worklist
				self._readRemoteDataAndCollectDepenencies(pendingIds, dependencyIdsToResolve, relKeysOnly = True)

	def __nameSearch(self, keyName, value, raiseOnMultiple):
		if not value:
			# do not search for empty names
			self.log("1_warning", "{} empty remote key value for field '{}'".format(
				self.modelName, keyName), modelName = self.modelName
			)
			return None

		localEntry = self.env[self.modelName].name_search(value, operator = '=')
		# if localEntry:
		# 	#_logger.info("__nameSearch({}, {}, {}) = {}".format(self.modelName, keyName, value, localEntry))
		if len(localEntry) > 1:
			msg = "Remote {} '{}' for {} maps to multiple local names:\n{}".format(
				keyName, value, self.modelName, localEntry
			)
			if raiseOnMultiple:
				raise ImportException(msg, modelName = self.modelName)
			else:
				self.log("1_warning", msg, modelName = self.modelName)

		return localEntry

	def __resolve(self):
		''' splits the provided id set in 3 distincs sets:
				1. ids we know the local id for
				2. ids not mappable to a local id
				3. ids we don't yet know if they belong into set 1 or set 2

				keyMaterial  -> resolve
		'''
		if not self.hasImportStrategy('match' ,'import'):
			raise Exception("__resolve({}) : should not be called for import strategy {}".format(
				self.modelName, self.importStrategy
		))

		theEnv 			= self.env[self.modelName]
		resolvedIds 	= set()
		unresolvedIds 	= set()
		pendingIds		= set()

		if not self.keyMaterial:
			return (resolvedIds, unresolvedIds, pendingIds)

		if self.keyMaterial.keys() & (self.toCreate.keys() | self.toUpdate.keys() | self.idMap.keys()):
			raise Exception(
				"Insonsistent worklist state for {}:\nkeyMaterial : {}\n2create    : {}\n2update    :{}\nidMap    :{}".format(
					self.modelName, self.keyMaterial.keys(), self.toCreate.keys(), self.toUpdate.keys(), self.idMap.keys()
			))

		relationKeyFieldsHandlers = {} # for fast access to handlers of id fields which are relations
		for idFieldName in self._getRemoteIdFields():
			relatedTypeName = self.getLocalFields()[idFieldName].get('relation')
			if relatedTypeName:
				relationKeyFieldsHandlers[idFieldName] = self.CTX.getHandler(relatedTypeName)

		pendingKeys = {}
		for remoteId, remoteKeys in self.keyMaterial.items():
			# if remoteId in self.idMap:
			# 	# already resolved. should not happen
			# 	_logger.warning("__resolve({}) : id {} is already resolved but still in keyMaterial?\n{}".format(
			# 		self.modelName, remoteId, self.keyMaterial
			# ))
			# 	resolvedIds.add(remoteId)
			# 	continue

			if self.matchingStrategy.startswith('odooName'):
				# if self.modelName == 'res.partner':
				# 	_logger.info("{} resolving {}".format(self.modelName, remoteKeys))
				localEntry = self.__nameSearch('display_name', remoteKeys['display_name'], raiseOnMultiple = True)

				# if we didn't find an exact hit for display_name try name if it exists if enabled
				# this is for types where name_get() is not the inverse of name_search(), like res.country.state
				if not localEntry and (self.matchingStrategy == 'odooNames') and ('name' in remoteKeys):
					localEntry = self.__nameSearch('name', remoteKeys['name'], raiseOnMultiple = True)

				if localEntry:
					self.idMapAdd(localEntry[0][0], remoteId)
					resolvedIds.add(remoteId)
				else:
					unresolvedIds.add(remoteId)

			elif self.matchingStrategy == 'explicitKeys':
				# if explicitly configured keys contain relation fields the ids might
				# have to stay pending until the relation field ids can be resolved
				cannotResolveRemoteRelationKey = False
				if relationKeyFieldsHandlers:
					# copy the keys and try to map their relation id values
					_originalRemoteKeys = dict(remoteKeys)
					for fieldName, handler in relationKeyFieldsHandlers.items():
						mappedId = handler.idMap.get(remoteKeys[fieldName])
						if mappedId:
							remoteKeys[fieldName] = mappedId
						else:
							# the remote relation key is not resolveable (yet) so this object is unresolved too
							cannotResolveRemoteRelationKey = True
							# _logger.info("{} cannot (yet) resolve key {} for remote id {} because of missing {} data".format(
							# 	self.modelName, remoteKeys, remoteId, handler.modelName#, handler.idMap
							# ))
							break

				if cannotResolveRemoteRelationKey:
					pendingKeys[remoteId] = _originalRemoteKeys
					continue

				# search for key locally
				domain = [
					[fn, '=', fv] for fn, fv in remoteKeys.items()
				]
				localEntry = theEnv.search(domain)
				if len(localEntry) < 1:
					unresolvedIds.add(remoteId)

				elif len(localEntry) == 1:
					self.idMapAdd(localEntry.id, remoteId)
					#_logger.info("resolved {} {} -> {}".format(self.modelName, remoteId, localEntry.id))
					resolvedIds.add(remoteId)

				else:
					raise Exception(
						"Remote keys {}.{} match multiple local objects for type {}:\n{}".format(
							remoteId, remoteKeys, self.modelName, localEntry
					))

			else:
				raise Exception("Unimplemented matching strategy '{}' for ".format(
					self.matchingStrategy, self.modelName)
				)

		#/end of per object loop

		# self.log('3_debug',
		_logger.info("{} resolve +{} -{} ?{}".format(self.modelName, len(resolvedIds), len(unresolvedIds), len(pendingKeys)))
		if self.keyMaterial.keys() ^ resolvedIds ^ unresolvedIds ^ pendingKeys.keys(): # sanity check
			raise Exception(
				"Calculated id sets do not add up to the given id set:\ninput      : {}\nresolved   : {}\nunresolved :{}\npending    :{}".format(
					self.keyMaterial.keys(), resolvedIds, unresolvedIds, pendingKeys.keys()
			))
		# fail if we have unrealoved keys in strategy 'match'
		if unresolvedIds and self.importStrategy == 'match':
			someUnresolvedKeys = map(self.keyMaterial.get, list(unresolvedIds)[:30])
			raise ImportException(
				"{} remote records of type {} could not be resolved locally:\n{}".format(
					len(unresolvedIds), self.modelName,
					# list the remoteKeys info for at most 30 of the failing ids
					"\n".join(map(str, someUnresolvedKeys))
				),
				# kwargs for log entry
				modelName = self.modelName, dependencyType = self.modelName
			)
		# done
		self.keyMaterial = pendingKeys
		return (resolvedIds, unresolvedIds, set(pendingKeys.keys()))

	def __getDiscoveryDomain(self):

		result = []
		tf = self.importConfig.getTimeFilterDomain()
		mf = self.modelConfig.model_remote_domain and eval(self.modelConfig.model_remote_domain) or False

		if mf:
			result.append(mf)

		if tf:
			result.append(tf)

		return result

	def fetchRemoteKeys(self, ids): #TODO: add remote consideration domain

		if ids == None: # check if intended read of ALL remote objects...
			if not self.hasImportStrategy('import', 'match'):
				# ... makes sense ...
				raise Exception("fetchALLRemoteKeys() called for importStrategy '{}' of type {}".format(self.importStrategy, self.modelName))
			if self.keyMaterial != None:
				# ... and occurs initially only
				raise Exception("fetchALLRemoteKeys() called twice or late for importStrategy '{}' of type {}".format(self.importStrategy, self.modelName))
			# ok, this seems to be a valid discover-all case. add configured search domain

		elif not ids:
			# forbid empty key lists since this would be a non-explcit read-ALL
			raise Exception("fetchRemoteKeys() called with empty set of ids. cowardly refusing to read ALL")

		if self.keyMaterial == None:
			self.keyMaterial = {}

		ids2Fetch = (ids and set(ids)) or ids

		#NOTE: ids2Fetch is either None (-> read all) or non-empty (-> read specific) from here

		if ids2Fetch: # specific ids are given
			# fetch only what we don't know yet
			ids2Fetch = ids2Fetch - self.idMap.keys() - self.toCreate.keys() - self.toUpdate.keys()
			if not ids2Fetch: # all present
				return ids or set(self.keyMaterial.keys())

		idFieldNames = self._getRemoteIdFields()
		idFieldsWhichAreRelations = { fn for fn in idFieldNames if self.getLocalFields()[fn].get('relation') }

		domain = self.__getDiscoveryDomain() if ids2Fetch == None else None
		# if domain:
		# 	_logger.info(str(domain))
		records = self.remoteOdoo.readData(self, idFieldNames, domain, self.log, ids = ids2Fetch)

		_logger.info("{} : read idFields {} of {} remote records (from {})".format(
			self.modelName, idFieldNames, len(records),
			domain or ("{} ids".format(len(ids2Fetch or [])))
		))

		if ids2Fetch and (len(ids2Fetch) != len(records)):
			raise Exception("{} : short read of {} items while trying to get remote names of {} ids".format(
				self.modelName, len(records), len(ids))
			)

		failOnRecordsWithAmbigousRemoteKeys = []
		for record in records:
			# build index
			remoteId = record.pop('id')
			self.keyMaterial[remoteId] = record

			# check uniqueness. create path of key values in remote self.remoteKeyUniquenessCheck and see if there is more than one object at the end of it
			#TODO: also check for empty key field values?
			#NOTE: the order in idFieldNames hast to be stable during the runtime of one import, otherwise this breaks
			node = self.remoteKeyUniquenessCheck # key path start
			for fn in idFieldNames:
				# # take id from [id, 'name'] returned for many2one-fields
				fieldValue = record[fn]
				if not fieldValue:
					pass
				# 	_logger.warning("{} key field '{}' for remote id {} is False in {}".format(self.modelName, fn, remoteId, record))
				elif fn in idFieldsWhichAreRelations:
					record[fn] = fieldValue[0]
				# walk/pave key path
				node = node.setdefault(record[fn], {})
			# insert record at end of key path
			node[remoteId] = record
			if len(node) > 1:
				# key path was taken before
				failOnRecordsWithAmbigousRemoteKeys.append((node.keys(), record))
				if len(failOnRecordsWithAmbigousRemoteKeys) > 30:
					break # enough for logging
				else:
					continue

			#NOTE: sanity check of field names in record...
			if idFieldNames ^ record.keys():
				# ... since it is data recieved from another process
				# ... and shall be passed directly into odoo search domains later on
				raise Exception(
					"Key info for {}::{} has non-requested fields (expected: {})".format(
						self.modelName, record, idFieldNames
				))

		if failOnRecordsWithAmbigousRemoteKeys:
			raise ImportException("{} : multiple remote object ids --> key combinations:\n{}".format(
				self.modelName, "\n".join(map(lambda x: "{} --> {}".format(list(x[0]), x[1]), failOnRecordsWithAmbigousRemoteKeys))
			), modelName = self.modelName)

		return ids or set(self.keyMaterial.keys())

	def readRemoteData(self, ids):
		''' reads to-be-imported data for a model from remote into self.remoteData '''
		if not ids:
			# # sanity check
			# if modelName in self.remoteData or self.getImportStrategy(modelName) != 'import':
				raise Exception(
					"readRemoteData() without specific ids is not allowed for type {}".format(self.modelName)
				)

		idsNotPresent = ids - self.remoteData.keys()

		if idsNotPresent:
			# read data for idsNotPresent into cache
			fieldNamesToImport = self.getFieldsToImport().keys()
			records = self.remoteOdoo.readData(self, fieldNamesToImport, None, self.log, ids = idsNotPresent)

			_logger.info("{} : read {} of {}/{} remote records with {} fields".format(
				self.modelName, len(records), len(idsNotPresent), len(ids), len(fieldNamesToImport)
			))

			if len(idsNotPresent) != len(records):
				raise Exception("Got {} records of remote model {} where we asked for {} ids".format(
					len(records), self.modelName, len(idsNotPresent)

				))

			many2oneFields = set([
				fieldName for fieldName, field in self.getRelFieldsToImport().items()
					if field['type'] == 'many2one'
			])

			# put into self.remoteData
			for record in records:
				self.remoteData[record['id']] = record
				# convert many2one fields into a list with one id (they come in as [id, displayName])
				for fn in many2oneFields:
					value = record[fn]
					if value:
						record[fn] = [value[0]]

		return { # return data for all requested ids from cache
			_id : self.remoteData[_id] for _id in ids
		}
