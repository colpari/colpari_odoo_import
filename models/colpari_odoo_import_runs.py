# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
import odoo.addons.decimal_precision as dp

import logging
import xmlrpc.client
import traceback

_logger = logging.getLogger(__name__)
_logLevelMap = {
	'3_debug' 	: logging.DEBUG,
	'2_info' 	: logging.INFO,
	'1_warning'	: logging.WARNING,
	'0_error' 	: logging.ERROR,
}

class ImportException(Exception):
	def __init__(self, message, **kwargs):
		self.kwargs = kwargs
		super(ImportException, self).__init__(message)

class OdooConnection():

	def __init__(self, url, username, dbname, password):
		try:
			self._dbName 	= dbname
			self._password 	= password
			_logger.info("Connecting to '{}'".format(url))
			self._common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
			_logger.info("Remote odoo version is '{}'".format(self._common.version()['server_version']))
			self._uid = self._common.authenticate(dbname, username, password, {})
			_logger.info("authenticated as uid '{}'".format(self._uid))
			self._models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
			self._fieldInfo = {}
		except Exception as e:
			raise UserError("Error connecting to odoo @ '{}' : {}".format(url, e))

	def modelCall(self, modelName, methodName, *args, **kwargs):
		#_logger.info("{}.modelCall({}, {}, {}, {})".format(self, modelName, methodName, args, kwargs))
		return self._models.execute_kw(self._dbName, self._uid, self._password, modelName, methodName, args, kwargs)

	def getFieldsOfModel(self, modelName):
		if modelName not in self._fieldInfo:
			self._fieldInfo[modelName] = self.modelCall(modelName, 'fields_get')
		return self._fieldInfo[modelName]

	def readData(self, modelName, fieldsToRead, _logFn, ids = None):
		''' read all objects or specific ids of a type '''
		specificIds = ids != None
		result = (
			self.modelCall(modelName, 'read', list(ids), fields = list(fieldsToRead))
				if ids != None else
			self.modelCall(modelName, 'search_read', [], fields = list(fieldsToRead))
		)
		_logFn('3_debug', "readData() : read {} remote {} records with {} ids given and {} fields".format(
				len(result), modelName, len(ids or []), len(fieldsToRead)
			), modelName=modelName
		)
		return result


class ImportContext():
	def __init__(self, importRun):
		self.env 			= importRun.env
		self.importRun 		= importRun
		self.log 			= importRun._log
		self.importConfig 	= importRun.import_config
		self._cc 			= {} # fast, indexed, config cache
		for modelConfig in self.importConfig.model_configs:
			self._cc[modelConfig.import_model.model] = {
				'config' : modelConfig,
				'fields' : {
					fieldConfig.import_field.name : fieldConfig
						for fieldConfig in modelConfig.field_configs
				}
			}

		connParams 			= self.importConfig.import_source
		self.remoteOdoo 	= OdooConnection( # make connection
			connParams.url, connParams.username, connParams.dbname, connParams.credential
		)
		self.remoteFields 		= {} # modelName -> fieldsDict
		self.localFields 		= {} # modelName -> fieldsDict
		self.fieldsToImport		= {} # modelName -> set(names)
		self._fieldProperties	= {
			# modelName : {
			#	propertyName : {
			#		'names'		: set(),
			#		'dicts'		: { fieldName : fieldDef },
			#		'values' 	: { fieldName : propertyValue }
			#	}
			# }
		}

		self.remoteData = {
			# modelName : {
			#	id : { field : value, ... }
			# }
		}

		self.keyMaps = {
			# modelName : {
			#	remoteId : {
			#		'remoteKeys' 	: { k :v, k :v }
			#		'localId'		: idOrNone
			#	}
			# }
		}

		self.importState = {
			# modelName : {
			#	'unresolvedIds' : { remoteId, remoteId, remoteId... },
			#   'keyMaterial'	: { remoteId : { k :v, k :v... }}
			#	'resolvedIds'	: { remoteId : localId }
			#	'toCreate'		: { remoteId : { k :v, k :v... } }
			#	'toUpdate'		: { remoteId : { k :v, k :v... } }
			# }
		}



		self.checkConfig()

	def _getModelEntry(self, modelName):
		return self._cc.get(modelName, {})

	def getModelConfig(self, modelName):
		return self._getModelEntry(modelName).get('config', None)

	def getFieldConfig(self, modelName, fieldName):
		me = self._getModelEntry(modelName)
		if me:
			return me.setdefault('fields', {}).get(fieldName, None)

	def getImportStrategy(self, modelName):
		mc = self.getModelConfig(modelName)
		return mc and mc.model_import_strategy or 'match'

	def getMatchingStrategy(self, modelName):
		mc = self.getModelConfig(modelName)
		# unconfigured models default to be matched by name
		return mc and mc.matching_strategy or 'odooName'

	# def isFieldIgnored(self, modelName, fieldName):
	# 	fc = self.getFieldConfig(modelName, fieldName)
	# 	return fc and fc.field_import_strategy == 'ignore'

	def getFieldImportStrategy(self, modelName, fieldName):
		if self.getImportStrategy(modelName) not in ('import', 'dependency'):
			return 'ignore'
		fc = self.getFieldConfig(modelName, fieldName)
		# unconfigured fields of imported types default to being imported
		return fc and fc.field_import_strategy or 'import'

	def shouldFollowDependency(self, modelName, fieldName):
		mc = self.getModelConfig(modelName)

		if not mc:
			raise Exception("ASSERT: shouldFollowDependency() must not be called with unconfigured type {}".format(modelName))

		if mc.model_import_strategy not in ('import', 'dependency'):
			raise Exception(
				"ASSERT: shouldFollowDependency() must not be called with non-imported type {} (strategy={})".format(
					modelName, mc.model_import_strategy
			))

		localField = self.getLocalFields(modelName)[fieldName]
		relatedType = localField.get('relation')
		if not relatedType:
			raise Exception(
				"ASSERT: shouldFollowDependency() must not be called with non-relation field {}.{}".format(
					modelName, fieldName
			))

		strategy = self.getImportStrategy(relatedType)
		if strategy == 'ignore':
			return False

		fc = self.getFieldConfig(modelName, fieldName)
		required = localField.get('required')
		_type = localField.get('type')
		#_logger.info("FOLLOW? {}.{} -> {}, {}, {}, {}".format(modelName, fieldName, _type, relatedType, fc, required))

		if _type == 'one2many':
			# always follow one2many TODO: ?? sure/configurable ??
			return relatedType

		if not required:
			if self.importConfig.only_required_dependencies or mc.only_required_dependencies:
				return False

		return relatedType


	def getLocalFields(self, modelName):
		result = self.localFields.get(modelName)
		if not result:
			result = self.localFields[modelName] = self.env[modelName].fields_get()
		return result

	def getRemoteFields(self, modelName):
		result = self.remoteFields.get(modelName)
		if not result:
			result = self.remoteFields[modelName] = self.remoteOdoo.getFieldsOfModel(modelName)
		return result

	def getFieldNamesToImport(self, modelName):
		strategy = self.getImportStrategy(modelName)
		if strategy not in ('import', 'dependency'):
			raise Exception("Code path error. getFieldNamesToImport() called for strategy {}".format(strategy))
		result = self.fieldsToImport.get(modelName)
		if not result:
			result = self.fieldsToImport[modelName] = set(self.getLocalFields(modelName).keys()).intersection(self.getRemoteFields(modelName))
		return result

	def _getPropertiesEntry(self, modelName, propertyName):
		entry = self._fieldProperties.setdefault(modelName, {}).setdefault(propertyName, {})
		if not entry:
			# initialize if empty
			names  = entry['names'] = set()
			dicts  = entry['dicts'] = {}
			values = entry['values'] = {}
			for fn, f in self.getLocalFields(modelName).items():
				propVal = f.get(propertyName)
				if propVal:
					names.add(fn)
					dicts[fn] = f
					values[fn] = propVal

		return entry

	def fieldsWhere(self, modelName, propertyName):
		return self._getPropertiesEntry(modelName, propertyName)['dicts']

	def fieldNamesWhere(self, modelName, propertyName):
		return self._getPropertiesEntry(modelName, propertyName)['names']

	def fieldProperties(self, modelName, propertyName):
		return self._getPropertiesEntry(modelName, propertyName)['values']

	def checkConfig(self):
		# check fields for all models we want to import
		for modelConfig in self.importConfig.model_configs:
			modelName = modelConfig.import_model.model
			iStrategy = modelConfig.model_import_strategy

			if modelConfig.matching_strategy == 'explicitKeys' and not modelConfig.getConfiguredKeyFields():
				raise ImportException("Matching strategy for {} is 'explicitKeys' but no key fields are configured".format(modelName))

			if iStrategy in ('ignore', 'match'):
				continue

			# read remote model
			remoteFields = self.getRemoteFields(modelName)
			localFields = self.getLocalFields(modelName)
			msg = "checking {}/{} local/remote fields".format(len(localFields), len(remoteFields))
			self.log('3_debug', msg, modelName=modelName)

			# determine which fields to import
			fieldsToImport = self.getFieldNamesToImport(modelName)

			# check if we should be able to provide all locally required fields
			for fieldName, field in localFields.items():
				required = field.get('required')
				relatedType = field.get('relation')
				ignored = self.getFieldImportStrategy(modelName, fieldName) == 'ignore'
				#_logger.info("checking field {} on {}; req={}, rel={}, ign={}".format(fieldName, modelName,required, relatedType, ignored))
				if ignored:
					fieldsToImport.discard(fieldName) #NOTE: this is a reference to self.fieldsToImport[modelName]

				if required:
					if ignored:
						raise ImportException("Field {}.{} is required but ignored in import configuration".format(modelName, fieldName))

					if fieldName not in fieldsToImport:
						fc = self.getFieldConfig(modelName, fieldName)
						if not fc or not fc.mapsToDefaultValue(): # do we have a default?
							raise ImportException(
								"Field {}.{} is required but not to be imported (not found on remote side?) and there is no default value configured".format(
								modelName, fieldName
							))

					if relatedType and self.getImportStrategy(relatedType) == 'ignore':
						raise ImportException("Field {}.{} is required but it contains type {} which is explicitly ignored in import configuration".format(
							modelName, fieldName, relatedType
						))


			unimportedFields = set(localFields.keys()) - fieldsToImport
			if unimportedFields:
				self.log('3_debug', "unimported fields : {}".format(unimportedFields), modelName=modelName)


	def doMatching(self):
		'''
			for import type
				- read remote keys
				- match locally and see which ones to import
				- find 'match' dependencies
					- queue fetching & resolving keys
				- find 'dependency' dependencies
					- queue fetching data

			for dependency entry:
				'match'
					fetch keys & resolve
				'dependency'
					- find 'match' dependencies
						- queue fetching & resolving keys
					- find 'dependency' dependencies
						- queue fetching data

		'''

		dependencyIdsToResolve = {} # dependencies of imported objects that need to be resolved

		# read remote keys of all main import types
		for modelConfig in self.importConfig.model_configs:
			modelName = modelConfig.import_model.model
			iStrategy = self.getImportStrategy(modelName)

			if iStrategy not in ('import', 'dependency'):
				continue

			keyMap = self._fetchRemoteKeys(modelName) #TODO: add remote consideration domain

			# resolve locally
			(resolvedIds, unresolvedIds) = self._resolve(modelName)

			# determine which remote objects we need to resolve dependencies of
			idsToImport = set()

			if modelConfig.do_create:
				idsToImport.update(unresolvedIds)

			if modelConfig.do_update:
				idsToImport.update(resolvedIds)

			if idsToImport:
				self._readRemoteDataAndDepenencies(modelName, idsToImport, dependencyIdsToResolve)

		# read key info for all dependencies and resolve them
		i = 0
		while dependencyIdsToResolve:
			i+=1
			thisPass = dependencyIdsToResolve
			dependencyIdsToResolve = {}

			for modelName, dependencyIds in thisPass.items():
				if not dependencyIds: # sanity check
					raise Exception("Empty dependency list")
				iStrategy = self.getImportStrategy(modelName)
				if iStrategy == 'ignore':
					raise Exception("Import strategy for {} should not be 'ignore' here".format(modelName))

				elif iStrategy == 'import':
					self._fetchRemoteKeys(modelName, ids = dependencyIds)
					(resolvedIds, unresolvedIds) = self._resolve(modelName)

				elif iStrategy == 'dependency':
					self._readRemoteDataAndDepenencies(modelName, dependencyIds, dependencyIdsToResolve)


				elif iStrategy == 'match':
					self._fetchRemoteKeys(modelName, ids = dependencyIds)
					(resolvedIds, unresolvedIds) = self._resolve(modelName)

					if unresolvedIds:
						keyMap = self.keyMaps[modelName]
						raise ImportException(
							"{} remote records of type {} could not be resolved locally:\n{}".format(
								len(unresolvedIds), modelName,
								# list the remoteKeys info for at most 30 of the failing ids
								"\n".join(map(lambda _id: str(keyMap[_id]['remoteKeys']), list(unresolvedIds)[:30]))
							),
							# kwargs for log entry
							modelName = modelName
						)
				else:
					raise Exception("Unhandled model import strategy '{}' for {}".format(iStrategy, modelName))

		_logger.info("doMatching() finished after {} iterations".format(i))

		# read match data for all dependency-only types with ids
		# resolve all keys for all dependency-only types

	def _readRemoteDataAndDepenencies(self, modelName, remoteIds, existingData = {}):
		# fetch remote data
		remoteData = self._readRemoteData(modelName, remoteIds)
		if not remoteData:
			return existingData

		fieldsToImport = self.getFieldNamesToImport(modelName)

		# check all relation fields
		for relationFieldName, relationField in self.fieldsWhere(modelName, 'relation').items():
			if relationFieldName not in fieldsToImport:
				continue

			relatedType = self.shouldFollowDependency(modelName, relationFieldName)
			if not relatedType:
				continue

			if self.getImportStrategy(relatedType) not in ('import', 'dependency'):
				self.log( # log message if type is not 
					'2_info', "following dependency {} -> {}".format(relationFieldName, relatedType),
					modelName = modelName, fieldName = relationFieldName, dependencyType = relatedType
				)

			dependencyIds = set()
			isMany2One = relationField['type'] == 'many2one'
			for remoteId in filter(None, map(lambda remoteDict : remoteDict[relationFieldName], remoteData.values())):
				if isMany2One:
					dependencyIds.add(remoteId[0])
				else:
					dependencyIds.update(remoteId)

			if dependencyIds:
				 existingData.setdefault(relatedType, set()).update(dependencyIds)

		return existingData

	def _resolve(self, modelName):
		iStrategy 		= self.getImportStrategy(modelName)
		mStrategy 		= self.getMatchingStrategy(modelName)
		theEnv 			= self.env[modelName]
		keyMap 			= self.keyMaps[modelName]
		idFieldNames 	= self._getRemoteIdFields(modelName)
		resolvedIds 	= set()
		unresolvedIds 	= set()

		for remoteId, data in keyMap.items():
			if data.get('localId'):
				resolvedIds.add(remoteId)
				continue # already resolved

			if iStrategy == 'dependency':
				# dependency data not resolved but created (and data['localId'] is set elsewhere)
				unresolvedIds.add(remoteId)
				continue

			remoteKeys = data['remoteKeys']

			#TODO: also check for empty key field values?
			#TODO: issue warning if multiple remote keys map to the same local key

			if mStrategy == 'odooName':
				localEntry = theEnv.name_search(remoteKeys['display_name'], operator = '=')
				if localEntry:
					if len(localEntry) == 1:
						_logger.info("resolved {} {} -> {}".format(modelName, remoteId, localEntry))
						data['localId'] = localEntry[0]
						resolvedIds.add(remoteId)
						continue
					else:
						raise ImportException(
							"Remote display_name '{}' for {} maps to multiple local names:\n{}".format(
								remoteKeys['display_name'], modelName, localEntry
						))

				if 'name' in remoteKeys:
					localEntry = theEnv.name_search(remoteKeys['name'], operator = '=')

				if localEntry:
					if len(localEntry) == 1:
						_logger.info("resolved {} {} -> {}".format(modelName, remoteId, localEntry))
						data['localId'] = localEntry[0]
						resolvedIds.add(remoteId)
						continue
					else:
						raise ImportException(
							"Remote name '{}' for {} maps to multiple local names:\n{}".format(
								remoteKeys['name'], modelName, localEntry
						))


				unresolvedIds.add(remoteId)

			elif mStrategy == 'explicitKeys':
				domain = [
					[fn, '=', fv] for fn, fv in remoteKeys.items()
				]
				localEntry = theEnv.search(domain)
				if len(localEntry) < 1:
					unresolvedIds(remoteId)

				elif len(localEntry) == 1:
					data['localId'] = localEntry.id
					resolvedIds(remoteId)

				else:
					raise Exception(
						"Multiple local matches ({}) for remote object {}.{} with keys {}".format(
							localEntry, modelName, remoteId ,remoteKeys
					))

		self.log('3_debug', "resolve +{} -{}".format(len(resolvedIds), len(unresolvedIds)), modelName=modelName)
		return (resolvedIds, unresolvedIds)

	def _fetchRemoteKeys(self, modelName, ids = None): #TODO: add remote consideration domain
		#FIXME: make really incremental
		keyMap = self.keyMaps.setdefault(modelName, {})
		if keyMap and ids == None:
			# we already have a keymap and no specific ids given. we are done
			return keyMap
		if ids != None:
			# check if we need to fetch more
			ids = set(filter(lambda i: i not in keyMap, ids))
			if not ids:
				return keyMap # all resolved already

		idFieldNames = self._getRemoteIdFields(modelName)
		records = self.remoteOdoo.readData(modelName, idFieldNames, self.log, ids = ids) #TODO: add remote consideration domain

		#_logger.info("_fetchRemoteKeys() : read idFields of {} remote records (from {} ids) of type {}".format(len(records), len(ids or []), modelName))

		if ids and (len(ids) != len(records)):
			raise Exception("Short read of {} items while trying to get remote names of {} models of type {}".format(
				len(records), len(ids), modelName)
			)

		for record in records:
			if idFieldNames ^ record.keys(): #NOTE: paranoia/sanity check of field names in record...
				# ... since it is data recieved from another process and passed directly into odoo search domains
				raise Exception(
					"Key info for {}::{} has non-matching fields. Expected {}".format(
						modelName, record, idFieldNames
				))

			x = keyMap[record['id']] = {
				'remoteKeys' 	: record,
				'localId' 		: None
			}
			#_logger.info("remote keys {}::{} = {}".format(modelName, record['id'], x))

		return keyMap

	def _getRemoteIdFields(self, modelName):
		''' determines the required fields for identifying the remote models  '''
		strategy = self.getMatchingStrategy(modelName)
		if strategy == 'odooName':
			fieldsToRead = { 'display_name', 'id' }
			if 'name' in self.getRemoteFields(modelName):
				fieldsToRead.add('name')

		elif strategy == 'explicitKeys':
			fieldsToRead = self.getModelConfig(modelConfig).getConfiguredKeyFieldNames()
			if not fieldsToRead:
				raise UserError("Model type {} has matching strategy 'explicitKey' but no key fields are configured")

		else:
			raise Exception("Model matching strategy '{}' is not supported".format(strategy))

		return fieldsToRead

	def _readRemoteData(self, modelName, ids): #TODO: add remote consideration domain
		''' reads to-be-imported data for a model from remote into self.remoteData '''
		ids = set(ids)
		if not ids:
			# sanity check
			if modelName in self.remoteData or self.getImportStrategy(modelName) != 'import':
				raise Exception(
					"_readRemoteData() without specific ids is only allowed once for non-import type name '{}'".format(modelName)
				)

		fieldsToImport = set(self.getFieldNamesToImport(modelName)) #NOTE: copy since this is cached
		# always read the id
		#fieldsToImport.add('id')# NEEDED?

		remoteData = self.remoteData.setdefault(modelName, {})

		idsNotPresent = ids - remoteData.keys()

		if idsNotPresent or not ids:
			records = self.remoteOdoo.readData(modelName, fieldsToImport, self.log, ids = idsNotPresent)
			for record in records:
				remoteData[record['id']] = record

			_logger.info("{} : read {}/{} remote records with {} fields".format(
				modelName, len(records), ids and len(ids) or 0, len(fieldsToImport)
			))

			if len(ids) != len(records):
				raise Exception("Got {} records of remote model {} where we asked for {} ids".format(len(records), len(ids), modelName))

		return remoteData


class colpariOdooImportRunMessage(models.Model):
	_name = 'colpari.odoo_import_run_message'
	_description = 'Import run diagnostic messages'
	_order = "write_date, level, text"
	_rec_name = 'text'

	import_run = fields.Many2one('colpari.odoo_import_run', required=True, ondelete='cascade')
	level = fields.Selection([('0_error', 'Error'), ('1_warning', 'Warning'), ('2_info', 'Info'),('3_debug', 'Debug')], required=True)
	text = fields.Char()

	model_name = fields.Char()
	field_name = fields.Char()
	dependency_type = fields.Char()

	def actionIgnoreRelatedType(self):
		self.ensure_one()
		if not self.dependency_type:
			raise ValidationError("Action not applicable")

		self.import_run.import_config.setModelConfig(
			self.dependency_type, {'model_import_strategy' : 'ignore'}
		)

		self.dependency_type = False # disable action / hide button

	def actionImportAsDependency(self):
		self.ensure_one()
		if not self.dependency_type:
			raise ValidationError("Action not applicable")

		self.import_run.import_config.setModelConfig(
			self.dependency_type, {'model_import_strategy' : 'dependency'}
		)

		self.dependency_type = False # disable action / hide button

	def actionImportFull(self):
		self.ensure_one()
		if not self.dependency_type:
			raise ValidationError("Action not applicable")

		self.import_run.import_config.setModelConfig(
			self.dependency_type, {'model_import_strategy' : 'import'}
		)

		self.dependency_type = False # disable action / hide button

	def actionIgnoreField(self):
		self.ensure_one()
		if not (self.model_name and self.field_name):
			raise ValidationError("Action not applicable")
		config 	= self.import_run.import_config
		mc 		= config.getModelConfig(self.model_name)
		if not mc:
			raise ValidationError("Model is not configured in import config - not adding field settings.")

		mc.setFieldConfig(self.field_name, {'field_import_strategy' : 'ignore'})

		self.field_name = False # disable action / hide button


class colpariOdooImportRun(models.Model):
	_name = 'colpari.odoo_import_run'
	_description = 'Import run'
	_help = 'A run of a specific import configuration'

	import_config = fields.Many2one('colpari.odoo_import_config', required=True, ondelete='cascade')

	state = fields.Selection(
		[('configure', 'Configure'), ('runnable', 'Runnable'), ('running', 'Running'), ('finished', 'Finished'), ('failed', 'Failed')],
		default='configure'
	)

	progress1 = fields.Integer()
	progress2 = fields.Integer()

	messages = fields.One2many('colpari.odoo_import_run_message', 'import_run', readonly=True)

	def _log(self, level, text, modelName=False, fieldName=False, dependencyType=False):
		self.ensure_one()
		_logger.log(_logLevelMap[level], text)
		self.messages.create([{
			'level' : level, 'text' : text, 'import_run' : self.id,
			'model_name' : modelName, 'field_name' : fieldName, 'dependency_type' : dependencyType
		}])

	def prepare(self):
		self.ensure_one()
		self.progress1 = 0
		self.progress2 = 0
		self.messages.unlink()
		self.progress2 = 5

		try:
			theImport = ImportContext(self)
			theImport.doMatching()

		except ImportException as ie:
			self._log('0_error', str(ie), **ie.kwargs)
			self.state = 'failed'

		except Exception as e:
			txt = traceback.format_exc()
			self._log('0_error', txt)
			self.state = 'failed'
			return None
