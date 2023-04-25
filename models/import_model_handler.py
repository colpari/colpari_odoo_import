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
		self.fieldsToImport		= None 	# lazy set(names)

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


	def getFieldNamesToImport(self):
		if self.fieldsToImport == None:
			if not self.hasImportStrategy('import', 'dependency'):
				raise Exception("Code path error. getFieldNamesToImport() called for strategy {}".format(strategy))

			self.fieldsToImport = set(self.getLocalFields().keys()).intersection(self.getRemoteFields().keys())

		return self.fieldsToImport

	def _getPropertiesEntry(self, propertyName):
		entry = self._fieldProperties.setdefault(propertyName, {})
		if not entry:
			# initialize if empty
			names  = entry['names'] = set()
			dicts  = entry['dicts'] = {}
			values = entry['values'] = {}
			for fn, f in self.getLocalFields().items():
				propVal = f.get(propertyName)
				if propVal:
					names.add(fn)
					dicts[fn] = f
					values[fn] = propVal

		return entry

	def fieldsWhere(self, propertyName):
		return self._getPropertiesEntry(propertyName)['dicts']

	def fieldNamesWhere(self, propertyName):
		return self._getPropertiesEntry(propertyName)['names']

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
		fieldsToImport = self.getFieldNamesToImport()

		# check if we should be able to provide all locally required fields
		for fieldName, field in localFields.items():
			required = field.get('required')
			relatedTypeName = field.get('relation')
			ignored = self.getFieldImportStrategy(fieldName) == 'ignore'
			#_logger.info("checking field {} on {}; req={}, rel={}, ign={}".format(fieldName, self.modelName, required, relatedTypeName, ignored))
			if ignored:
				# NOTE: this mutates self.fieldsToImport[modelName]
				# 	thus, this method is part of the is-proper-set-up path for this class
				# 	and must be called right after all ImportModelHandler instances are created
				fieldsToImport.discard(fieldName)

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

		unimportedFields = set(localFields.keys()) - fieldsToImport
		if unimportedFields:
			self.log('3_debug', "unimported fields : {}".format(unimportedFields), modelName=self.modelName)

		return True

	def _getDependenciesOfRecords(self, records):

		fieldsToImport = self.getFieldNamesToImport()
		relFields = list(filter(
			lambda x: x[0] in fieldsToImport,
			self.fieldsWhere('relation').items()
		))
		result = {}

		for relationFieldName, relationField in relFields:

			relatedType = self.shouldFollowDependency(relationFieldName)
			if not relatedType:
				continue

			isMany2One = relationField['type'] == 'many2one'
			dependencyIds = set()
			for remoteId, remoteRecord in records.items():
				relationFieldValue = remoteRecord[relationFieldName]
				if isMany2One:
					dependencyIds.add(relationFieldValue[0])
				else:
					dependencyIds.update(relationFieldValue)

			if dependencyIds:
				result.setdefault(relatedType, set()).update(dependencyIds)

		return result

	def __mapDependenciesOfRecords(self, records):
		''' maps ids in all dependency fields of all records to local values '''
		fieldsToImport = self.getFieldNamesToImport()
		relFields = list(filter(
			lambda x: x[0] in fieldsToImport and self.shouldFollowDependency(x[0]), #TODO: too scrummed
			self.fieldsWhere('relation').items()
		))

		if not relFields:
			_logger.info("{}.__mapDependenciesOfRecords() without relFields?".format(self.modelName))
			# nothing to resolve. return a copy
			return {
				_id : dict(_record)
					for _id, _record in records.items()
			}

		result = {}

		for relationFieldName, relationField in relFields:

			relatedType = self.shouldFollowDependency(relationFieldName)
			isMany2One = relationField['type'] == 'many2one'
			dependencyIds = set()
			for remoteId, remoteRecord in records.items():
				relationFieldValue = remoteRecord[relationFieldName]

				ids2Map = [relationFieldValue[0]] if isMany2One else relationFieldValue

				mappedIds = list(map(relatedType.idMap.get, ids2Map))
				if len(ids2Map) != len(mappedIds):
					raise ValidationError("Not all ids found?")

				# copy/edit remoteRecord in/to result
				result.setdefault(remoteId, dict(remoteRecord))[relationFieldName] = (
					mappedIds[0] if isMany2One else mappedIds
				)

		if len(records) != len(result): # sanity check
			raise ValidationError("I did not copy&return all records for {}? ({}/{})\nrecords: {}\n\nresult: {}".format(
				self.modelName, len(records), len(result), records, result
			))

		return result

	def _readRemoteDataAndCollectDepenencies(self, remoteIds, dependencyIdsToResolve):
		# fetch remote data
		remoteData = self.readRemoteData(remoteIds)

		fieldsToImport = self.getFieldNamesToImport()

		dependencies = self._getDependenciesOfRecords(remoteData)

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
					modelName = self.modelName
				)

		elif self.importStrategy == 'dependency':
			ids2Read = ids - self.toCreate.keys()
			remoteData = self._readRemoteDataAndCollectDepenencies(ids2Read, dependencyIdsToResolve)
			self.toCreate.update(remoteData)

		else:
			raise Exception("Unhandled model import strategy '{}' for {}".format(self.importStrategy, self.modelName))

	def writeRecursive(self, typesSeen = set()):

		result = 0 # number of objects changed
		for objectSet, operation in ((self.toCreate, 'create'), (self.toUpdate, 'update')):
			tttypesSeen = set(typesSeen)
			tttypesSeen.add(self)
			writeResult = self.__writeRecursive(tttypesSeen, objectSet, operation)
			result + writeResult

		return result

	def __keySelect(self, key):
		return lambda x : x[key]

	def __toDictionary(self, keyName, iterable):
		result = {}
		for x in iterable:
			result.setdefault(x[keyName], x)
		return result

	def __writeRecursive(self, typesSeen, objectSet, operation):
		'''
			for update/create
				find all ids we depend on
				check if resolved
				if not recurse to dependency
		'''
		mappedIds = {}
		dependencies = self._getDependenciesOfRecords(objectSet)
		# see which ones are completely resolved and remove them
		for handler, dependencyIds in dict(dependencies).items():
			(yes, no) = handler.resolve(dependencyIds)
			if not no: # all resolved
				mappedIds[handler.modelName] = handler.idMap
				del dependencies[handler]

		# any unresolved dependencies left?
		if not dependencies:
			_logger.info("{} {} RESOLVE COMPLETE".format(self.modelName, operation))
			if operation == 'update':
				# map ids of objectset
				mappedIds = map(self.idMap.get, map(self.__keySelect('id'), objectSet))
				# locally fetch objects with mapped ids
				localObjects = self.__toDictionary('id',
					self.env[self.modelName].browse(mappedIds)
				)
				# check size
				if len(localObjects) != len(objectSet): # sanity check
					raise ValidationError("Got {} local objects when browsing for {} ids".format(lenb(localObjects), len(objectSet)))
				# map ids in all dependency fields
				objectSetMapped = self.__mapDependenciesOfRecords(objectSet)
				# loop and write per id
				for localId, dataToUpdate in objectSetMapped:
					localObjects[localId].update(dataToUpdate)
				_logger.info("UPDATED {} {} records".format(len(objectSet), self.modelName))
				return len(objectSet) # return number of objects changed

			elif operation == 'create':
				objectSetMapped = self.__mapDependenciesOfRecords(objectSet)
				createResult = self.env[self.modelName].create(list(objectSetMapped.values()))
				_logger.info("CREATED {} {} records".format(len(objectSetMapped), self.modelName))
				return len(createResult) # return number of objects changed
			else:
				raise ValidationError("Unsupported operation : '{}'".format(operation))


		# we have unresolved dependencies. recurse to them to see if we can write them
		result = 0
		for handler, dependencyIds in dependencies.items():
			if handler in typesSeen:
				# _logger.info("{} {} : has {} unresolved dependencies to {} - NOT recursing circle".format(
				# 	operation, self.modelName, len(dependencyIds), handler.modelName
				# ))
				continue
			else:
				_logger.info("{} {} : has {} unresolved dependencies to {} - recursing".format(
					operation, self.modelName, len(dependencyIds), handler.modelName
				))
				result += handler.writeRecursive(typesSeen) # return number of objects changed

		return result

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
			fieldsToImport = self.getFieldNamesToImport()
			records = self.remoteOdoo.readData(self.modelName, fieldsToImport, self.log, ids = idsNotPresent)

			_logger.info("{} : read {} of {}/{} remote records with {} fields".format(
				self.modelName, len(records), len(idsNotPresent), len(ids), len(fieldsToImport)
			))

			if len(idsNotPresent) != len(records):
				raise Exception("Got {} records of remote model {} where we asked for {} ids".format(
					len(records), self.modelName, len(idsNotPresent)

				))

			for record in records:
				self.remoteData[record['id']] = record



		return {
			_id : self.remoteData[_id] for _id in ids
		}

