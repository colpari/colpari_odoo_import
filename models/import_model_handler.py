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

	def _readRemoteDataAndCollectDepenencies(self, remoteIds, dependencyIdsToResolve):
		# fetch remote data
		remoteData = self.readRemoteData(remoteIds)

		fieldsToImport = self.getFieldNamesToImport()

		# check all relation fields
		for relationFieldName, relationField in self.fieldsWhere('relation').items():
			if relationFieldName not in fieldsToImport:
				continue

			relatedType = self.shouldFollowDependency(relationFieldName)
			if not relatedType:
				continue

			if not relatedType.modelConfig:
				self.log( # log message if type is not configured
					'2_info', "following dependency {} -> {}".format(relationFieldName, relatedType.modelName),
					modelName = self.modelName, fieldName = relationFieldName, dependencyType = relatedType.modelName
				)

			dependencyIds = set()
			isMany2One = relationField['type'] == 'many2one'
			for remoteId in filter(None, map(lambda remoteDict : remoteDict[relationFieldName], remoteData.values())):
				if isMany2One:
					dependencyIds.add(remoteId[0])
				else:
					dependencyIds.update(remoteId)

			if dependencyIds: # update dependencyIdsToResolve if we have any news
				 dependencyIdsToResolve.setdefault(relatedType, set()).update(dependencyIds)

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
		return "{} : {} keys, {} resolved, {} to create, {} to update".format(
			self.modelName, len(self.keyMaterial or []), len(self.idMap), len(self.toUpdate), len(self.toCreate), 
		)

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
		_logger.info("{} resolve +{} -{}".format(self.modelName, len(resolvedIds), len(unresolvedIds)))#, modelName=self.modelName)
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

