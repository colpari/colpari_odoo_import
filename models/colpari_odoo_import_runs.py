# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
import odoo.addons.decimal_precision as dp

import logging
import xmlrpc.client
import traceback

_logger = logging.getLogger(__name__)
_logLevelMap = {
	'debug' 	: logging.DEBUG,
	'info' 		: logging.INFO,
	'warning' 	: logging.WARNING,
	'error' 	: logging.ERROR,
}


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
		return self._models.execute_kw(self._dbName, self._uid, self._password, modelName, methodName, args, kwargs)

	def getFieldsOfModel(self, modelName):
		if modelName not in self._fieldInfo:
			self._fieldInfo[modelName] = self.modelCall(modelName, 'fields_get')
		return self._fieldInfo[modelName]

	def readData(self, modelName, fieldsToRead, ids = None):
		''' read all objects or specific ids of a type '''
		specificIds = ids != None
		result = (
			self.modelCall(modelName, 'read', list(ids), fields = list(fieldsToRead))
				if ids != None else
			self.modelCall(modelName, 'search_read', [], fields = list(fieldsToRead))
		)
		_logger.info("read {} remote records with {} ids given and fields {}".format(len(result), len(ids or []), fieldsToRead))
		return result


class ImportContext():
	def __init__(self, importRun):
		self.env 			= importRun.env
		self.importRun 		= importRun
		self.log 			= importRun._log
		self.importConfig 	= importRun.import_config
		self._cc 			= {} # fast, indexed, config cache
		for modelConfig in self.importConfig.model_configs:
			_cc[modelConfig.import_model.model] = {
				'config' : modelConfig,
				'fields' : {
					fieldConfig.import_field.name : fieldConfig
						for fieldConfig in modelConfig.field_configs
				}
			}

		connParams 			= self.import_config.import_source
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

	def isFieldIgnored(self, modelName, fieldName):
		fc = self.getFieldConfig(modelName, fieldName)
		return fc and fc.field_import_strategy == 'ignore'

	def getImportStrategy(self, modelName, fieldName):
		if self.getImportStrategy(modelName) != 'import':
			return 'ignore'
		fc = self.getFieldConfig(modelName, fieldName)
		# unconfigured fields of imported types default to being imported
		return fc and fc.field_import_strategy or 'import'

	def getLocalFields(self, modelName):
		result = self.localFields.get(modelName)
		if not result:
			result = self.localFields[modelName] = self.env[modelName].fields_get()
		return result

	def getRemoteFields(self, modelName):
		result = self.remoteFields.get(modelName)
		if not result:
			result = self.remoteFields[modelName] = remoteOdoo.getFieldsOfModel(modelName)
		return result

	def _getPropertiesEntry(self, modelName, propertyName):
		entry = self._fieldProperties.setdefault(modelName, {}).setdefault(propertyName, {})
		if not entry:
			# initialize if empty
			names  = entry['names'] = set()
			dicts  = entry['dicts'] = {}
			values = entry['values'] = {}
			for fn, f in self.getLocalFields(modelName):
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
		for modelConfig in self._import_config.model_configs:
			modelName = modelConfig.import_model.model

			if modelConfig.matching_strategy == 'explicitKeys' and not modelConfig.getConfiguredKeyFields():
				raise Exception("Matching strategy for {} is 'explicitKeys' but not key fields are configured".format(modelName))

			if modelConfig.model_import_strategy == 'ignore':
				continue

			# read remote model
			remoteFields = self.getRemoteFields(modelName)
			localFields = self.getLocalFields(modelName)
			msg = "checking {}/{} local/remote fields for type {}".format(len(localFields), len(remoteFields), modelName)
			self.log('debug', msg)

			# determine which fields to import
			fieldsToImport = set(localFields.keys()).intersection(remoteFields.keys()) - self.fieldNamesWhere(modelName, 'readonly')

			# check if we should be able to provide all locally required fields
			for fieldName, field in localFields.items():
				required = field.get('required')
				relatedType = field.get('relation')
				ignored = self.getImportStrategy(modelName, fieldName) == 'ignore'

				if ignored:
					fieldsToImport.remove(fieldName)

				if required:
					if ignored:
						raise Exception("Field {}.{} is required but ignored in import configuration".format(modelName, fieldName))

					if fieldName not in fieldsToImport:
						fc = self.getFieldConfig(modelName, fieldName)
						if not fc or not fc.mapsToDefaultValue(): # do we have a default?
							raise Exception(
								"Field {}.{} is required but not to be imported (not found on remote side?) and there is no default value configured".format(
								modelName, fieldName
							))

					if relatedType and self.getImportStrategy(relatedType) == 'ignore':
						raise Exception("Field {}.{} is required but it contains type {} which is explicitly ignored in import configuration".format(
							modelName, fieldName, relatedType
						))


			self.fieldsToImport[modelName] = fieldsToImport
			self.log('info', "local fields that will NOT be imported for {} : {}".format(modelName, set(localFields.keys()) - fieldsToImport))

			'''
				TODO: alignment
					- read remote import types info
					- deduct remote dependency info
					- read local import & dependency types info
					- check local sense
				. odo level 0.13...  ideally...
					- read import types data (yields dependency DB keys)
					- fetch depKeys resolve-info
					- try to reolve locally

					- start somewhere
			'''

	def doMatching(self):
		''' ??? '''

		# read remote keys of all types we want to create or update
		for modelConfig in self._import_config.model_configs:
			modelName = modelConfig.import_model.model
			if self.getImportStrategy(modelName) != 'import':
				continue

			self._fetchRemoteKeys(modelName)

			# resolve locally
			# determine which data we need from remote
			# fetch remote data 
			# deduct dependencies

			self._readRemoteData(modelName)
			idMap = self.idMaps[modelName] = {}
			# resolve these


			# find dependencies to all non-written types (leafes)
			for relationFieldName, relationField in self.fieldsWhere(modelName, 'relation').items():
				if relationFieldName not in self.fieldsToRead[modelName]:
					continue

				relatedType = relationField['relation']
				if self.CC.isModelImported(relatedType):
					# not a leaf but a top level import
					continue

				# find all dependency ids to map
				idMap = self.idMaps.setdefault(relatedType, {})
				isMany2One = relationField['type'] == 'many2one'
				for remoteId in filter(None, map(lambda remoteDict : remoteDict[relationFieldName], self.remoteData[modelName])):
					if isMany2One:
						#if remoteId:
							idMap[remoteId[0]] = None
					else:
						for _id in remoteId:
							idMap[_id] = None

		for modelName, idMap in self.idMaps.items():
			self.log('debug', "")

		# read match data for all dependency-only types with ids
		# resolve all keys for all dependency-only types

	def _fetchRemoteKeys(self, modelName, ids = None):
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
		records = self.remoteOdoo.readData(modelName, idFieldNames, ids = ids)

		for record in records:
			keyMap[record['id']] = {
				'remoteKeys' 	: record,
				'localId' 		: None
			}

		return keyMap

	def _getRemoteIdFields(self, modelName):
		''' determines the required fields for identifying the remote models  '''
		if modelName in self.remoteData:
			raise Exception("_readRemoteData() called twice for name '{}'".format(modelName))

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

	def _readRemoteData(self, modelName, ids = None):
		''' determines the required fields for the model and reads them from remote into self.remoteData  '''
		if modelName in self.remoteData:
			raise Exception("_readRemoteData() called twice for name '{}'".format(modelName))

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

		# if we import this type read all import fields now too
		fieldsToRead += self.fieldsToImport.get(modelName, set())
		# always read the id
		fieldsToRead.add('id')

		records = self.remoteOdoo.readData(modelName, fieldsToRead, ids = ids)
		self.remoteData[modelName] = records
		self.log('info', "read {} records with {} fields, ids = {}".format(len(records), len(fieldsToRead), ids and len(ids) or None))


class colpariOdooImportRunMessage(models.Model):
	_name = 'colpari.odoo_import_run_message'
	_description = 'Import run diagnostic messages'
	_order = "write_date, level, text"
	_rec_name = 'text'

	import_run = fields.Many2one('colpari.odoo_import_run', required=True, ondelete='cascade')
	level = fields.Selection([('debug', 'Debug'), ('info', 'Info'), ('warning', 'Warning'), ('error', 'Error')], default='configure')
	text = fields.Char()


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

	def _log(self, level, text):
		self.ensure_one()
		_logger.log(_logLevelMap[level], text)
		self.messages.create([{'level':level, 'text':text, 'import_run':self.id}])

	def _checkRunnable(self):
		'''
			connect to remote and verify models. move to state runnable if successful.
			for efficiency reasons we return the remote connection we have to make anyways, since we might be part of a _run()

			TODO: alignment
					- read remote import types info
					- deduct remote dependency info
					- read local import & dependency types info
					- check local sense
				. odo level 0.13...  ideally...
					- read import types data (yields dependency DB keys)
					- fetch depKeys resolve-info
					- try to reolve locally

				- start somewhere


		'''
		def isImportable(propDict):
			return not propDict.get('readonly')# and propDict.get('store')

		self.ensure_one()
		self.progress1 = 0
		self.progress2 = 0
		self.messages.unlink()
		self.progress2 = 5
		connParams = self.import_config.import_source
		# create connection
		remoteOdoo = OdooConnection(
			connParams.url, connParams.username, connParams.dbname, connParams.credential
		)
		# for my config's model_configs
		modelsToImport = self.import_config.model_configs
		modelsToImportCount=len(modelsToImport)
		modelsProcessed = 0

		theImport = ImportContext(self)

		theImport.doMatching()

# 		leavesToResolve = {
# 			# 'model' : set(remoteId)
# 		}

# 		CC = self.import_config._getCC() # indexed config cache
# 		for modelConfig in modelsToImport:
# 			modelName = modelConfig.import_model.model
# 			mc = CC.getModelConfig(modelName)

# 			if mc.model_import_strategy == 'ignore':
# 				continue

# 			# read remote data and check if we can resolve keys
# 			#TODO: save remote data. besides names its all we need from here
# 			remoteData = remoteOdoo.modelCall(modelName, 'search_read', [], fields = list(fieldsToRead))
# 			#_logger.info(remoteData)
# 			_logger.info("read {} remote models for {} using {} of {} local fields".format(len(remoteData), modelName, len(fieldsToRead), len(localFields)))
# z

# 			modelsProcessed+=1
# 			self.progress2 = int(
# 				5 + ((modelsProcessed/modelsToImportCount)*95)
# 			)

# 		for modelName, perModel in leavesToResolve.items():
# 			mc = CC.getModelConfig(modelName)
# 			if mc and mc.model_import_strategy in ('full', 'create'):
# 				# process types we might create later
# 				continue

# 			matchingStrategy = mc.matching_strategy if mc else 'odooName'
# 			self._log('debug', "trying to map {} leaf objects of type {} with strategy {}".format(len(perModel), modelName, matchingStrategy))
# 			_logger.info("ids = {}".format(perModel))
# 			if matchingStrategy == 'odooName':
# 				self._resolveByName(remoteOdoo, modelName, perModel)
# 			elif matchingStrategy == 'explicitKeys':
# 				pass
# 			else:
# 				raise UserError("Unsupported matching strategy '{}' for model type {}".format(matchingStrategy, modelName))

# 		# return odoo connection
		return remoteOdoo

	def _resolveByName(self, remoteOdoo, modelName, remoteIds):
		fieldsToRead = ['display_name', 'id']
		remoteFields = remoteOdoo.getFieldsOfModel(modelName)
		if 'name' in remoteFields:
			fieldsToRead.append('name')
		remoteNamesAndIds = remoteOdoo.modelCall(modelName, 'read', list(remoteIds), fields = fieldsToRead)
		if len(remoteNamesAndIds) != len(remoteIds):
			raise Exception("Short read of {} items while trying to get remote names of {} models of type {}".format(
				len(remoteNamesAndIds), len(remoteIds), modelName)
			)
		theEnv = self.env[modelName]
		result = {}
		for remoteEntry in remoteNamesAndIds:
			localEntry = theEnv.name_search(remoteEntry['display_name'], operator = '=')
			if localEntry:
				result[remoteEntry['id']] = localEntry[0]
				continue

			if 'name' in remoteEntry:
				localEntry = theEnv.name_search(remoteEntry['name'], operator = '=')

			if localEntry:
				result[remoteEntry['id']] = localEntry[0]
				continue

			self._log('warning', "No local entry of type {} found for {}".format(modelName, remoteEntry))

		return result


	def prepare(self):
		'''
			move to state runnable if _checkRunnable()
		'''
		self.ensure_one()
		try:
			remoteConnection = self._checkRunnable()
			if remoteConnection:
				self.state = 'runnable'
			return remoteConnection
		except Exception as e:
			txt = traceback.format_exc()
			self._log('error', txt)
			self.state = 'failed'
			return None


	def _run(self):
		''' _prepare() if runnable, read remote data '''
		self.ensure_one()

		odooConnection = self.prepare()

		if not odooConnection:
			return None
			#TODO: return warning


		# theorectically looks good
		# build local dependency graph
		# for graph.leafes
			# import or die

