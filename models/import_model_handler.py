# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

import logging
import xmlrpc.client
import traceback

_logger = logging.getLogger(__name__)
_logLevelMap = {
	'3_debug' 	: logging.INFO, #logging.DEBUG,
	'2_info' 	: logging.INFO,
	'1_warning'	: logging.WARNING,
	'0_error' 	: logging.ERROR,
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
		self.fieldsToImport		= None 	# lazy set((fn, f))
		self.relFieldsToImport	= None 	# lazy set((fn, f))

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

	def __str__(self):
		return "<{} handler>".format(self.modelName)

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
			self.localFields = self.env[self.modelName].fields_get()
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

			for fn, f in localFields.items():
				if fn not in remoteFields:
					continue
				if f.get('relation') and not self.shouldFollowDependency(fn):
					continue
				self.fieldsToImport[fn] = f

		return self.fieldsToImport

	def getRelFieldsToImport(self):
		if self.relFieldsToImport == None:
			fieldsToImport = self.getFieldsToImport()
			if not self.hasImportStrategy('import', 'dependency'):
				raise Exception("Code path error. getRelFieldsToImport() called for strategy {}".format(strategy))

			self.relFieldsToImport = { fn : f for fn, f in fieldsToImport.items() if f.get('relation')}

		return self.relFieldsToImport

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
				del fieldsToImport[fieldName]

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

		result = {}

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
				relationFieldValue = remoteRecord[relationFieldName]
				if relationFieldValue:
					dependencyIds.update(relationFieldValue)

			if dependencyIds:
				result.setdefault(relatedType, set()).update(dependencyIds)

		return result

	def __mapDependenciesOfRecords(self, records, requiredOnly):
		''' - copy records and map remote ids in all relation fields to local ids
			- if requiredOnly remove all non-required relation fields from result
			- convert many2One fields to single values since we handle them as lists internally
		'''
		relFields = self.getRelFieldsToImport()

		#NOTE: yes, this would be much faster if the outer loop is relFields and records is inner...
		#		...but this way it has fewer border cases
		result = {}
		for remoteId, remoteRecord in records.items():
			destination = result[remoteId] = dict(remoteRecord) # copy
			for relationFieldName, relationField in relFields.items():
				if requiredOnly and not self.getLocalFields()[relationFieldName].get('required'): # maybe remove nonRequired field
					del destination[relationFieldName]
				else: # map rest
					relatedType = self.shouldFollowDependency(relationFieldName) # should never be False here
					relationFieldValue = destination[relationFieldName]
					if relationFieldValue:
						mappedIds = []
						for remoteRelatedId in relationFieldValue:
							localId = relatedType.idMap.get(remoteRelatedId)
							if not localId:
								raise ValidationError("{} could not map remote id {}\nout of: {}".format(
									relatedType.modelName, remoteRelatedId, relatedType.idMap
								))
							mappedIds.append(localId)

						destination[relationFieldName] = (
							mappedIds[0] # convert many2One fields to single values since we handle them as lists internally
								if relationField.get('type') == 'many2one' else
							mappedIds
						)

		if len(records) != len(result): # sanity check
			raise ValidationError("I did not copy&return all records for {}? ({}/{})\nrecords: {}\n\nresult: {}".format(
				self.modelName, len(records), len(result), records, result
			))

		return result

	def __removeResolvedDependenciesFrom(self, dependencies):
		# see which ones are completely resolved and remove them
		for handler, dependencyIds in dict(dependencies).items():
			(yes, no) = handler.resolve(dependencyIds)
			if not no: # all resolved
				del dependencies[handler]

	def _readRemoteDataAndCollectDepenencies(self, remoteIds, dependencyIdsToResolve):
		# fetch remote data
		remoteData = self.readRemoteData(remoteIds)

		#TODO: are there cases where we could go with requiredOnly = True and thus save time?
		dependencies = self._getDependenciesOfRecords(remoteData, requiredOnly = False)

		for _type, ids in dependencies.items():
			dependencyIdsToResolve.setdefault(_type, set()).update(ids)

		return remoteData

	def _getRemoteIdFields(self):
		''' determines the required fields for identifying the remote models  '''

		if self.matchingStrategy == 'odooName':
			fieldsToRead = { 'display_name', 'id' }
			if 'name' in self.getRemoteFields():
				fieldsToRead.add('name')

		elif self.matchingStrategy == 'explicitKeys':
			fieldsToRead = self.modelConfig.getConfiguredKeyFieldNames()
			if not fieldsToRead:
				raise UserError("Model type {} has matching strategy 'explicitKey' but no key fields are configured")

		else:
			raise Exception("Model matching strategy '{}' is not supported for {}".format(self.matchingStrategy, self.modelName))

		return fieldsToRead

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
		return "{} ({}/{}): {} keys, {} resolved, {} to update, {} to create (checkSum={})".format(
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
			(resolvedIds, unresolvedIds) = self.resolve(ids)

			# determine which remote objects we need to read
			idsToImport = set()

			if self.modelConfig.do_create:
				idsToImport.update(unresolvedIds)

			if self.modelConfig.do_update:
				idsToImport.update(resolvedIds)

			idsToImport = idsToImport - self.toCreate.keys() - self.toUpdate.keys() - self.idMap.keys()

			if idsToImport:
				remoteData = self._readRemoteDataAndCollectDepenencies(idsToImport, dependencyIdsToResolve)
				for remoteId, remoteRecord in remoteData.items():
					if remoteId in unresolvedIds:
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
			(resolvedIds, unresolvedIds) = self.resolve(ids)
			# all must be resolved
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

	def __keySelect(self, key):
		return lambda x : x[key]

	def __toDictionary(self, keyName, iterable):
		result = {}
		for x in iterable:
			#TODO: assert keys unique?
			result.setdefault(x[keyName], x)
		return result

	def tryCreate(self):

		if not self.toCreate:
			return True

		dependencies = self._getDependenciesOfRecords(self.toCreate, requiredOnly = True)

		if dependencies:
			_logger.info("{} create dependencies 1:\n{}".format(self.modelName, dependencies))

		self.__removeResolvedDependenciesFrom(dependencies)

		# any unresolved dependencies left?
		if dependencies:
			# we cannot write
			_logger.info("{} has unresolved dependencies to {} objects of {} types".format(
				self.modelName, sum(map(lambda x: len(x), dependencies.values())), len(dependencies.keys())
			))
			return False

		# nope - let's go!
		_logger.info("{} create RESOLVE (mandatory) COMPLETE, writing {} records".format(
			self.modelName, len(self.toCreate)
		))

		objectSetMapped = self.__mapDependenciesOfRecords(self.toCreate, requiredOnly = True)
		recordsToCreate = list(objectSetMapped.values())

		# __mapDependenciesOfRecords(requiredOnly = True) removed all non-required dependency fields (if any)...
		# ... put them into the 'update' stage (self.toUpdate)
		optionalRelatedFields = self.fieldNamesWhereNot('required') & self.getRelFieldsToImport().keys()
		updatesScheduled = 0
		if optionalRelatedFields:
			for remoteId, unMappedRecord in self.toCreate.items():
				if remoteId in self.toUpdate: # sanity check
					raise ValidationError("{} remote id {} should not yet be present in self.toUpdate".format(
						self.modelName, remoteId
					))

				updateData = self.toUpdate[remoteId] = {'id' : remoteId}

				#NOTE: using copy of UNmapped value for later update (because it will map again)
				for fn in optionalRelatedFields:
					updateData[fn] = list(unMappedRecord[fn])

				updatesScheduled+=1

		createResult = self.env[self.modelName].create(recordsToCreate)
		if len(recordsToCreate) != len(createResult): # sanity check
			raise ValidationError("Got {} created records when trying to create {}".format(
				len(createResult), len(recordsToCreate)
			))
		# add ids of newly created records to idMap
		# NOTE: this assumes model.create() returns the created records in the same order as the supplied data.
		#		which proves to be true for odoo 14 and 15 at least :-}
		# FIXME: unit test
		i = 0
		for created in createResult:
			self.idMap[recordsToCreate[i]['id']] = created['id']
			i+=1

		_logger.info("CREATED {} {} records ({} updates scheduled)".format(len(recordsToCreate), self.modelName, updatesScheduled))
		self.toCreate.clear()
		return len(createResult) # return True/number of objects changed

	def tryUpdate(self):
		if not self.toUpdate:
			return True

		dependencies = self._getDependenciesOfRecords(self.toUpdate, requiredOnly = False)
		self.__removeResolvedDependenciesFrom(dependencies)

		# any unresolved dependencies left?
		if dependencies:
			# we cannot write
			raise ValidationError("Unresolved dependencies in update stage for {}?\n{}".format(self.modelName, dependencies))
			#return False

		# nope - let's go!
		_logger.info("{} update RESOLVE COMPLETE, writing {} records".format(
			self.modelName, len(self.toUpdate)
		))

		objectSetMapped = self.__mapDependenciesOfRecords(self.toUpdate, requiredOnly = False)
		# map ids of objectSetMapped
		mappedIds = list(map(self.idMap.get, map(self.__keySelect('id'), objectSetMapped)))
		# locally fetch objects with mapped ids
		localObjects = self.__toDictionary('id',
			self.env[self.modelName].browse(mappedIds)
		)
		# check size
		if len(localObjects) != len(self.toUpdate): # sanity check
			raise ValidationError("Got {} local objects when browsing for {} ids".format(len(localObjects), len(self.toUpdate)))
		# loop and update local objects per id
		for remoteId, dataToUpdate in objectSetMapped:
			localId = self.idMap[remoteId]
			localObjects[localId].update(dataToUpdate)

		recordsWritten = len(self.toUpdate)

		_logger.info("UPDATED {} {} records".format(recordsWritten, self.modelName))
		self.toUpdate.clear()
		return recordsWritten

	def resolve(self, ids):
		theEnv 			= self.env[self.modelName]
		resolvedIds 	= set()
		unresolvedIds 	= set()

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
				domain = [
					[fn, '=', fv] for fn, fv in remoteKeys.items()
				]
				localEntry = theEnv.search(domain)
				if len(localEntry) < 1:
					unresolvedIds(remoteId)

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
		return (resolvedIds, unresolvedIds)

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

		ids2Fetch = (ids and set(ids)) or ids
		if ids2Fetch and self.keyMaterial: # specific ids given and we already have key data. fetch incremental
			ids2Fetch = set(filter(lambda i : i not in self.keyMaterial, ids)) # check which ones we don't have
			if not ids2Fetch:
				return ids # all present

		idFieldNames = self._getRemoteIdFields()
		records = self.remoteOdoo.readData(self.modelName, idFieldNames, self.log, ids = ids2Fetch) #TODO: add remote consideration domain

		_logger.info("fetchRemoteKeys() : read idFields of {} remote records (from {} ids) of type {}".format(
			len(records), len(ids2Fetch or []), self.modelName)
		)

		if ids2Fetch and (len(ids2Fetch) != len(records)):
			raise Exception("Short read of {} items while trying to get remote names of {} models of type {}".format(
				len(records), len(ids), self.modelName)
			)

		for record in records:
			# build index
			self.keyMaterial[record['id']] = record

			# NOTE: sanity check of field names in record...
			if idFieldNames ^ record.keys():
				# ... since it is data recieved from another process
				# ... and shall be passed directly into odoo search domains later on
				raise Exception(
					"Key info for {}::{} has non-matching fields. Expected {}".format(
						self.modelName, record, idFieldNames
				))

			#_logger.info("remote keys {}::{} = {}".format(self.modelName, record['id'], x))

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

