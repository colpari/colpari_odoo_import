# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

import logging
import xmlrpc.client
import traceback
import time

_logger = logging.getLogger(__name__)
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
	'product.template' : {
		# this is a real circle. since product.template manages the product.product instances we make them non-required and ignore them
		'product_variant_ids' : { 'required' : False }
	}
}

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

		self.modelConfig 	= self.importConfig.getModelConfig(modelName)
		self.fieldConfigs	= {}

		if self.modelConfig:
			for fieldConfig in self.modelConfig.field_configs:
				self.fieldConfigs[fieldConfig.import_field.name] = fieldConfig
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
			# 	'names'		: set(),
			# 	'dicts'		: { fieldName : fieldDef },
			# 	'values' 	: { fieldName : propertyValue }
			# }
		}

		# FIXME prefix these internal fields with __
		self.remoteData = {
			# id : { field : value, ... }
		}

		self.keyMaterial	= None 	# { remoteId : { k :v, k :v... }}
		self.idMap			= {} 	# { remoteId : localId }
		self.toCreate		= {} 	# { remoteId : { k :v, k :v... } }
		self.toUpdate		= {} 	# { remoteId : { k :v, k :v... } }

		# keeps track of all remote key combinations to assure they are unique in the first place
		self.remoteKeyUniquenessCheck = {} # {  kv1 : { kv2 : { k3v .. : set(remoteId) }}}

	def __repr__(self):
		return "<{} handler>".format(self.modelName)

	def __keySelect(self, key):
		return lambda x : x[key]

	def __toDictionary(self, keyName, iterable):
		result = {}
		for x in iterable:
			#TODO: assert keys unique?
			result.setdefault(x[keyName], x)
		return result

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

	def getRemoteFields(self):
		if not self.remoteFields:
			self.remoteFields = self.CTX.remoteOdoo.getFieldsOfModel(self.modelName)
		return self.remoteFields

	def shouldFollowDependency(self, fieldName):
		if not self.modelConfig:
			raise Exception("ASSERT: shouldFollowDependency() must not be called with unconfigured type {}".format(self.modelName))

		if not self.hasImportStrategy('import', 'dependency'):
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
		_type = localField.get('type')

		if relatedType.importStrategy == 'ignore':
			if required:
				raise ImportException("Type {} is required as dependency for {}.{} but ignored".format(
					relatedType.modelName, self.modelName, fieldName,
					modelName=self.modelName, fieldName=fieldName, dependencyType=relatedType.modelName
				))
			return False

		if _type == 'one2many':
			# always follow one2many TODO: sure?/configurable?
			return relatedType

		if not required:
			if self.importConfig.only_required_dependencies or self.modelConfig.only_required_dependencies:
				return False

		return relatedType


	def getFieldsToImport(self):
		if self.fieldsToImport == None:
			if not self.hasImportStrategy('import', 'dependency'):
				raise Exception("Code path error. getFieldsToImport() called for strategy {}".format(strategy))

			self.fieldsToImport = {}
			localFields = self.getLocalFields()
			remoteFields = self.getRemoteFields()
			followedNotified = set()
			for fn, f in localFields.items():
				if fn not in remoteFields:
					continue
				if f.get('related'):
					continue
				relatedTypeName = f.get('relation')
				if relatedTypeName:
					handler = self.shouldFollowDependency(fn)
					if not handler:
						continue
					elif not handler.modelConfig and handler not in followedNotified:
						# log message if relatedTypeName is not configured (we implicitly follow it) once
						self.log(
							'2_info', "following dependency {}::{} -> {}".format(self.modelName, fn, relatedTypeName),
							modelName = self.modelName, fieldName = fn, dependencyType = relatedTypeName
						)
						followedNotified.add(handler) # log once only

				self.fieldsToImport[fn] = f

		return self.fieldsToImport

	def getRelFieldsToImport(self):
		if self.relFieldsToImport == None:
			if not self.hasImportStrategy('import', 'dependency'):
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
				and self.CTX.getHandler(field['relation']).hasImportStrategy('import', 'dependency')):
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

	def checkConfig(self):
		''' NOTE: must be called right after all ImportModelHandler instances are created '''
		if not self.modelConfig:
			return True

		if self.matchingStrategy == 'explicitKeys' and not self.modelConfig.getConfiguredKeyFields():
			raise ImportException("Matching strategy for {} is 'explicitKeys' but no key fields are configured".format(self.modelName))

		if self.hasImportStrategy('ignore', 'match'):
			# check only types we read/write data of
			return True

		# read remote model
		remoteFields = self.getRemoteFields()
		localFields = self.getLocalFields()
		#msg = "checking {}/{} local/remote fields".format(len(localFields), len(remoteFields))
		#self.log('3_debug', msg, modelName=self.modelName)

		# determine which fields to import
		fieldsToImport = self.getFieldsToImport()

		# check if we should be able to provide all locally required fields
		for fieldName, field in localFields.items():
			required = field.get('required')
			relatedTypeName = field.get('relation')
			ignored = self.getFieldImportStrategy(fieldName) == 'ignore'
			#_logger.info("checking field {} on {}; req={}, rel={}, ign={}".format(fieldName, self.modelName, required, relatedTypeName, ignored))
			if ignored:
				# NOTE: this mutates self.fieldsToImport
				# 	thus, this method is part of the is-proper-set-up path for this class
				# 	and must be called right after all ImportModelHandler instances are created
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

		if not fieldsToImport:
			raise ImportException(
				"Empty import field list for {}".format(self.modelName), modelName=self.modelName
			)

		unimportedFields = set(localFields.keys()) - fieldsToImport.keys()
		if unimportedFields:
			self.log('3_debug', "unimported fields : {}".format(unimportedFields), modelName=self.modelName)

		return True

	def _getDependenciesOfRecords(self, records, requiredOnly):

		dependenciesPerTargetType = {}
		dependenciesPerId = {}

		relFieldNames = self.getRelFieldsToImport().keys()

		if requiredOnly:
			relFieldNames = set(relFieldNames) & self.fieldNamesWhere('required')

		for relationFieldName in relFieldNames:
			relatedType = self.shouldFollowDependency(relationFieldName)
			if not relatedType:
				raise ValidationError(
					"This path should not be taken since relFieldsToImport is already filtered by shouldFollowDependency()??"
				)

			dependencyIds = set()
			for remoteId, remoteRecord in records.items():
				relationFieldValue = remoteRecord.get(relationFieldName)
				if relationFieldValue:
					dependencyIds.update(relationFieldValue)
					dependenciesPerId.setdefault(remoteId, {}).setdefault(relatedType, set()).update(relationFieldValue)

			if dependencyIds:
				dependenciesPerTargetType.setdefault(relatedType, set()).update(dependencyIds)

		return (dependenciesPerTargetType, dependenciesPerId)

	def __mapRecords(self, records, requiredDependenciesOnly):
		''' - copy records and map remote ids in all relation fields to local ids
			- if requiredDependenciesOnly remove all non-required relation fields from result
			- apply configured field value mappings
			- convert many2One fields to single values since we handle them as lists internally
		'''
		relFields = self.getRelFieldsToImport()
		result = {}
		# copy all records
		for remoteId, remoteRecord in records.items():
			destination = result[remoteId] = dict(remoteRecord) # copy

		for fieldName, field in self.getFieldsToImport().items():
			if fieldName in relFields:
			# relation field
				if requiredDependenciesOnly and not field.get('required'):
					# remove non-required relations if requested
					for remoteRecord in result.values():
						remoteRecord.pop(fieldName, 0)
				else:
					# map relation field
					relatedType = self.shouldFollowDependency(fieldName) # should never be False here
					for remoteRecord in result.values():
						relationFieldValue = remoteRecord.get(fieldName)
						if relationFieldValue:
							mappedIds = list(map(relatedType.idMap.__getitem__, relationFieldValue))
							remoteRecord[fieldName] = (
								mappedIds[0] # convert many2One fields to single values since we handle them as lists internally
									if field.get('type') == 'many2one' else
								mappedIds
							)


			else:
			# normal field
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

	def __removeResolvedDependenciesFrom(self, dependenciesPerTargetType, dependenciesPerId = None):
		# see which ones are completely resolved and remove them
		for handler, dependencyIds in dict(dependenciesPerTargetType).items():
			(yes, no, pending) = handler.resolve(dependencyIds)
			if not (no or pending):
			# all resolved
				del dependenciesPerTargetType[handler]
				if dependenciesPerId:
					for remoteId, dependencyInfo in dependenciesPerId.items():
						dependencyInfo.pop(handler, 0)
			elif yes and dependenciesPerId:
			# at lease some are resolved. remove from dependenciesPerId
				for remoteId, dependencyInfo in dependenciesPerId.items():
					_deps = dependencyInfo.get(handler)
					if _deps:
						_deps.difference_update(yes) # remove resolved
						if not _deps: # all resolved
							del dependencyInfo[handler]

		# remove (now) empty entries from dependenciesPerId
		if dependenciesPerId:
			for remoteId, dependencyInfo in dict(dependenciesPerId.items()).items():
				if not dependencyInfo:
					del dependenciesPerId[remoteId]

	def _readRemoteDataAndCollectDepenencies(self, remoteIds, dependencyIdsToResolve):
		# fetch remote data
		remoteData = self.readRemoteData(remoteIds)

		#TODO: are there cases where we could go with requiredOnly = True and thus save time?
		(dependenciesPerTargetType, dependenciesPerId) = self._getDependenciesOfRecords(remoteData, requiredOnly = False)

		for _type, ids in dependenciesPerTargetType.items():
			dependencyIdsToResolve.setdefault(_type, set()).update(ids)

		return remoteData

	def _getRemoteIdFields(self):
		''' determines the required fields for identifying the remote models  '''
		if self.remoteIdFields == None:
			if self.matchingStrategy == 'odooName':
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

	def __nameSearch(self, keyName, value):
		if not value:
			# do not search for empty names
			return None

		localEntry = self.env[self.modelName].name_search(value, operator = '=')
		if localEntry:
			#_logger.info("__nameSearch({}, {}, {}) = {}".format(self.modelName, keyName, value, localEntry))
			if len(localEntry) > 1:
				raise ImportException(
					"Remote {} '{}' for {} maps to multiple local names:\n{}".format(
						keyName, value, self.modelName, localEntry
				))

			return localEntry[0]

		return None

	def status(self):
		checkSum = set(self.idMap.keys()) ^ (self.keyMaterial and self.keyMaterial.keys() or set()) ^ self.toUpdate.keys() ^ self.toCreate.keys()
		return "{} \t({}/{}): \t{} keys, \t{} resolved, \t{} to update, \t{} to create \t(checkSum={})".format(
			self.modelName, self.importStrategy, self.matchingStrategy,
			len(self.keyMaterial or []), len(self.idMap), len(self.toUpdate), len(self.toCreate), len(checkSum)
		)

	def isFinished(self):
		return not (self.toCreate or self.toUpdate)

	def hasContent(self):
		return self.toCreate or self.toUpdate or self.keyMaterial or self.idMap

	def readIncremental(self, ids, dependencyIdsToResolve):
		if not ids:
			raise Exception("Empty id list")

		if self.importStrategy == 'ignore':
			raise Exception("Import strategy for {} should not be 'ignore' here".format(self.modelName))

		elif self.importStrategy == 'import':
			self.fetchRemoteKeys(ids)
			(resolvedIds, unresolvedIds, pendingIds) = self.resolve(ids)

			# determine which remote objects we need to read
			idsToImport = set(pendingIds)

			if self.modelConfig.do_create:
				idsToImport.update(unresolvedIds)

			if self.modelConfig.do_update:
				idsToImport.update(resolvedIds)

			idsToImport = idsToImport - self.toCreate.keys() - self.toUpdate.keys() - self.idMap.keys()

			if idsToImport:
				# read ids to import and put into toCreate/toUpdate
				remoteData = self._readRemoteDataAndCollectDepenencies(idsToImport, dependencyIdsToResolve)
				for remoteId, remoteRecord in remoteData.items():
					if remoteId in unresolvedIds or remoteId in pendingIds:
						self.toCreate[remoteId] = remoteRecord
					elif remoteId in resolvedIds:
						self.toUpdate[remoteId] = remoteRecord
					else: # sanity check
						raise Exception(
							"ID {} for {} should be either in resolved or unresolved ids:\nrequested: {}\nresolved: {}\nunresolved: {}".format(
								remoteId, self.modelName, ids, resolvedIds, unresolvedIds
							)
						)
		elif self.importStrategy == 'match':
			self.fetchRemoteKeys(ids)
			(resolvedIds, unresolvedIds, pendingIds) = self.resolve(ids)
			# all must be resolved or pending
			if unresolvedIds:
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

		elif self.importStrategy == 'dependency':
			ids2Read = ids - self.toCreate.keys()
			remoteData = self._readRemoteDataAndCollectDepenencies(ids2Read, dependencyIdsToResolve)
			self.toCreate.update(remoteData)

		else:
			raise Exception("Unhandled model import strategy '{}' for {}".format(self.importStrategy, self.modelName))

	def __createIntro(self):
		''' preparations before the actual create run '''
		# # remove data from o2m-fields if we also import the target type of
		# # we needed to read this to discover all dependencies but for writing, these relations should be written by
		# # the corresponding m2o-field
		# # FIXME: this needs only to be done at the first create run
		# o2mFieldsWeImportTargetsOf = self.__getO2MFieldsWeImportTargetsOf()
		# if o2mFieldsWeImportTargetsOf:
		# 	for remoteRecord in self.toCreate.values():
		# 		for fn in o2mFieldsWeImportTargetsOf.keys():
		# 			remoteRecord.pop(fn, 0)

		# update us with the actual resolving situation four ourselves. it might still to be decided if some objects get
		# created or updated
		(resolvedIds, unresolvedIds, pendingIds) = self.resolve(self.toCreate.keys())
		for resolvedId in resolvedIds: # resolved ones go to update
			if resolvedId in self.toUpdate: raise Exception(
				"{} id {} should not yet be present in self.toUpdate()".format(self.modelName, resolvedId)
			)
			self.toUpdate[resolvedId] = self.toCreate[resolvedId]

		return pendingIds

	def tryCreate(self):

		if not self.toCreate:
			return True

		pendingIds = self.__createIntro()
		if pendingIds:
			_logger.info("{} has {} pending ids".format(self.modelName, len(pendingIds)))
			return False


		# fetch dependencies of self.toCreate
		(dependenciesPerTargetType, dependenciesPerId) = self._getDependenciesOfRecords(self.toCreate, requiredOnly = True)
		# remove resolved dependencies
		self.__removeResolvedDependenciesFrom(dependenciesPerTargetType, dependenciesPerId)

		# check which objects are fully resolved now and can be created
		_toCreate = { }
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
				self.modelName, sum(map(lambda x: len(x), dependenciesPerTargetType.values())), list(dependenciesPerTargetType.keys())
			))
			return False

		# we can at least write some records
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
				if remoteId in self.toUpdate: # sanity check
					raise ValidationError("{} remote id {} should not yet be present in self.toUpdate".format(
						self.modelName, remoteId
					))

				updateData = {}

				for fn in optionalRelatedFields:
					#NOTE: using copy of UNmapped value for later update (because it will map again)
					unmappedValue = unMappedRecord[fn]
					if unmappedValue:
						updateData[fn] = list(unmappedValue)

				if not updateData:
					# all relfields are empty
					pass
				else:
					self.toUpdate[remoteId] = updateData
					updatesScheduled+=1

		try:
			createResult = self.env[self.modelName].create(recordsToCreate)
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
		for created in createResult:
			self.idMap[recordsToCreate[i]['id']] = created['id']
			i+=1

		_logger.info("CREATED {}/{} {} records ({} updates scheduled)".format(
			len(recordsToCreate), len(self.toCreate), self.modelName, updatesScheduled
		))

		self.toCreate = _toTryLater
		return len(createResult) # return True/number of objects changed

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

	def __logUpdateObjectFailed(self, localId, localObject, remoteId, dataToUpdate, e):
		currentObjContent = str(
			{ fn : localObject[fn] for fn in localObject._fields.keys() }
		)
		msg = "update for {}.{} (remoteId={}) with data {} FAILED: {}\ncurrent content:{}".format(
				self.modelName, localId, remoteId, dataToUpdate, e, currentObjContent
			)

		if localObject._name == 'account.move.line':
			for line in localObject.move_id.line_ids:
				msg+="\n{} cred={}, deb={}, bal={}".format(line, line.credit, line.debit, line.balance)
			csum = sum(localObject.move_id.line_ids.mapped('credit'))
			dsum = sum(localObject.move_id.line_ids.mapped('debit'))
			bsum = sum(localObject.move_id.line_ids.mapped('debit'))
			msg+="\ncsum={}, dsum={}, bsum={}, +/- c/d = {}".format(csum, dsum, bsum, csum-dsum)

		return msg

	def tryUpdate(self):
		if not self.toUpdate:
			return True

		(dependenciesPerTargetType, dependenciesPerId) = self._getDependenciesOfRecords(self.toUpdate, requiredOnly = False)
		self.__removeResolvedDependenciesFrom(dependenciesPerTargetType)

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
		localObjects = self.__toDictionary('id',
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
			try:
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
				msg = self.__logUpdateObjectFailed(localId, localObject, remoteId, dataToUpdate, e)
				if failed == 30:
					raise ImportException(msg, modelName = self.modelName)
				else:
					_logger.error(msg)

		recordsWritten = len(self.toUpdate)

		_logger.info("UPDATED {} {} records".format(recordsWritten, self.modelName))
		self.toUpdate.clear()
		return recordsWritten

	def resolve(self, ids):
		theEnv 			= self.env[self.modelName]
		resolvedIds 	= set()
		unresolvedIds 	= set()
		pendingIds		= set()

		relationKeyFieldsHandlers = {} # for fast access to handlers of id fields which are relations
		for idFieldName in self._getRemoteIdFields():
			relatedType = self.getLocalFields()[idFieldName].get('relation')
			if relatedType:
				relationKeyFieldsHandlers[idFieldName] = self.CTX.getHandler(relatedTypeName)

		for remoteId in ids:
			if remoteId in self.idMap:
				# already resolved
				resolvedIds.add(remoteId)
				continue

			if self.importStrategy == 'dependency':
				# dependency data is not resolved but is created elsewhere)
				unresolvedIds.add(remoteId)
				continue

			remoteKeys = self.keyMaterial[remoteId]

			#TODO: also check for empty key field values?
			#TODO: issue warning if multiple remote keys map to the same local key

			if self.matchingStrategy == 'odooName':
				localEntry = (
					self.__nameSearch('display_name', remoteKeys['display_name'])
						or 'name' in remoteKeys and
					self.__nameSearch('name', remoteKeys['name'])
				)

				if localEntry:
					self.idMap[remoteId] = localEntry[0]
					_logger.info("resolved {} {} -> {}".format(self.modelName, remoteId, localEntry))
					resolvedIds.add(remoteId)
				else:
					unresolvedIds.add(remoteId)

			elif self.matchingStrategy == 'explicitKeys':
				# if explicitly configured keys contain relation fields the ids might stay pending until the remote
				# relation field ids can be resolved
				cannotResolveRemoteRelationKey = False
				if relationKeyFieldsHandlers:
					remoteKeys = dict(remoteKeys)
					for fieldName, handler in relationKeyFieldsHandlers.items():
						mappedId = handler.idMap.get(remoteKeys[fieldName])
						if mappedId:
							remoteKeys[fieldName] = mappedId
						else:
							# the remote relation key is not resolveable (yet) so this object is unresolved too
							cannotResolveRemoteRelationKey = True
							_logger.info("{} cannot (yet) resolve key {} for remote id {} because of missing {} data".format(
								self.modelName, remoteKeys, remoteId, handler
							))
							break

				if cannotResolveRemoteRelationKey:
					pendingIds.add(remoteId)
					continue

				domain = [
					[fn, '=', fv] for fn, fv in remoteKeys.items()
				]
				localEntry = theEnv.search(domain)
				if len(localEntry) < 1:
					unresolvedIds.add(remoteId)

				elif len(localEntry) == 1:
					self.idMap[remoteId] = localEntry.id
					_logger.info("resolved {} {} -> {}".format(self.modelName, remoteId, localEntry.id))
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

		#self.log('3_debug',
		#_logger.info("{} resolve +{} -{}".format(self.modelName, len(resolvedIds), len(unresolvedIds)))#, modelName=self.modelName)
		if ids ^ resolvedIds ^ unresolvedIds: # sanity check
			raise Exception("Calculated id sets do not add up to the given id set:\ninput     : {}\nresolved  : {}\nunresolved:{}".format(
				ids, resolvedIds, unresolvedIds
			))
		return (resolvedIds, unresolvedIds, pendingIds)

	def fetchRemoteKeys(self, ids): #TODO: add remote consideration domain

		if ids == None: # check if intended read of ALL remote objects...
			if not self.hasImportStrategy('import', 'match'):
				# ... makes sense ...
				raise Exception("fetchALLRemoteKeys() called for importStrategy '{}' of type {}".format(self.importStrategy, self.modelName))
			if self.keyMaterial != None:
				# ... and occurs initially only
				raise Exception("fetchALLRemoteKeys() called twice or late for importStrategy '{}' of type {}".format(self.importStrategy, self.modelName))
		elif not ids:
			# forbid empty key lists since they would lead to an implicit read-all
			raise Exception("fetchALLRemoteKeys() called with empty set of ids")

		if self.keyMaterial == None:
			self.keyMaterial = {}

		ids2Fetch = (ids and set(ids)) or ids #NOTE: ids2Fetch may be None here and stay None in a read-all case

		if ids2Fetch and self.keyMaterial: # specific ids given and we already have key data. fetch incremental
			ids2Fetch = ids2Fetch - self.keyMaterial.keys() # check which ones we don't have
			if not ids2Fetch: # all present
				return ids or set(self.keyMaterial.keys())

		idFieldNames = self._getRemoteIdFields()
		records = self.remoteOdoo.readData(self.modelName, idFieldNames, self.log, ids = ids2Fetch) #TODO: add remote consideration domain

		# _logger.info("fetchRemoteKeys() : read idFields of {} remote records (from {} ids) of type {}".format(
		# 	len(records), len(ids2Fetch or []), self.modelName)
		# )

		if ids2Fetch and (len(ids2Fetch) != len(records)):
			raise Exception("Short read of {} items while trying to get remote names of {} models of type {}".format(
				len(records), len(ids), self.modelName)
			)

		for record in records:
			remoteId = record['id']

			# build index
			self.keyMaterial[remoteId] = record
			del record['id']

			# check uniqueness. create key values path in remote self.remoteKeyUniquenessCheck
			node = self.remoteKeyUniquenessCheck
			for fn in idFieldNames:
				node = node.setdefault(record[fn], {})
			node[remoteId] = 1
			if len(node) > 1:
				_logger.warning("{} : multiple remote object ids ({}) have the same set of keys ({})".format(
					self.modelName, node.keys(), record
				))

			# NOTE: sanity check of field names in record...
			if idFieldNames ^ record.keys():
				# ... since it is data recieved from another process
				# ... and shall be passed directly into odoo search domains later on
				raise Exception(
					"Key info for {}::{} has non-matching fields. Expected {}".format(
						self.modelName, record, idFieldNames
				))

		return ids or set(self.keyMaterial.keys())

	def readRemoteData(self, ids): #TODO: add remote consideration domain
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
			records = self.remoteOdoo.readData(self.modelName, fieldNamesToImport, self.log, ids = idsNotPresent)

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
				# convert many2one fields into a list with one id (they come as [id, displayName])
				for fn in many2oneFields:
					value = record[fn]
					if value:
						record[fn] = [value[0]]

		return { # return data for all requested ids from cache
			_id : self.remoteData[_id] for _id in ids
		}

