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
			_logger.info("UID is '{}'".format(self._uid))
			self._models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
		except Exception as e:
			raise UserError("Error connecting to odoo @ '{}' : {}".format(url, e))

	def modelCall(self, modelName, methodName, *args, **kwargs):
		return self._models.execute_kw(self._dbName, self._uid, self._password, modelName, methodName, args, kwargs)

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
		'''
		def isImportable(propDict):
			return not propDict.get('readonly') and propDict.get('store')

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
		leavesToResolve = {
			# 'model' : set(remoteId)
		}
		allRemoteFields = {}
		CC = self.import_config._getCC() # indexed config cache
		for modelConfig in modelsToImport:
			modelName = modelConfig.import_model.model
			mc = CC.getModelConfig(modelName)
			if mc.model_import_strategy == 'ignore':
				continue
			# read remote model
			remoteFields = allRemoteFields[modelName] = remoteOdoo.modelCall(modelName, 'fields_get')
			localFields = self.env[modelName].fields_get()

			# determine which fields to read from remote
			fieldsToRead = set(localFields.keys()).intersection(remoteFields.keys()).intersection(filter(lambda fn : isImportable(localFields[fn]), localFields))

			# for fn, f in remoteFields.items():
			# 	_logger.info("remote field {} : {}".format(fn, f))
			# for fn, f in localFields.items():
			# 	_logger.info("local  field {} : {}".format(fn, f))

			localRelatedFields = dict(filter(lambda i : i[1].get('relation'), localFields.items()))
			msg = "checking {}/{} local/remote fields for type {}".format(len(localFields), len(remoteFields), modelName)
			_logger.info(msg)
			#_logger.info(localFields)
			self._log('debug', msg)

			# for local field:
			for localFieldName, localField in localFields.items():
				fc = CC.getFieldConfig(modelName, localFieldName)
				#_logger.info("checking {}/{} = {}".format(modelName, localFieldName, fc))

				if localField.get('required'):
					#_logger.info("checking required {}/{} = {}".format(modelName, localField, fc))

					if fc and fc.field_import_strategy == 'ignore':
						raise Exception("Field {}.{} is required but ignored in import configuration".format(modelName, localFieldName))

					relatedType = localField.get('relation')
					if relatedType:
						rmc = CC.getModelConfig(relatedType)
						if rmc and rmc.model_import_strategy == 'ignore':
							raise Exception(
								"Field {}.{} is required but it contains type {} which is ignored in import configuration".format(
									modelName, localFieldName, relatedType
								)
							)

					if localFieldName not in remoteFields:
						# remote field not present
						if not fc or not fc.mapsToDefaultValue(): # do we have a default?
							raise Exception("Field {}.{} is required but not found on remote side and there is no default value set".format(
								modelName, localFieldName
							))

			if mc.model_import_strategy == 'ignore':
				continue
			# fieldsToRead = [
			# 	fieldName if
			# ]
			# read remote data and check if we can resolve keys
			remoteData = remoteOdoo.modelCall(modelName, 'search_read', [], fields = list(fieldsToRead))
			#_logger.info(remoteData)
			_logger.info("read {} remote models for {} using {} of {} local fields".format(len(remoteData), modelName, len(fieldsToRead), len(localFields)))
			for relationFieldName, relationField in localRelatedFields.items():
				if relationFieldName not in fieldsToRead:
					continue
				relatedType = relationField['relation']
				if CC.isFieldIgnored(modelName, relationFieldName) or CC.isModelIgnored(relatedType):
					# we ignore the field or the target model
					continue
				# find all ids to map
				idMap = leavesToResolve.setdefault(relatedType, set())
				isMany2One = relationField['type'] == 'many2one'
				for remoteId in filter(None, map(lambda remoteDict : remoteDict[relationFieldName], remoteData)):
					if isMany2One:
						if remoteId:
							idMap.add(remoteId[0])
						else:
							for _id in remoteId:
								idMap.add(_id)

			modelsProcessed+=1
			self.progress2 = int(
				5 + ((modelsProcessed/modelsToImportCount)*95)
			)

		for modelName, perModel in leavesToResolve.items():
			mc = CC.getModelConfig(modelName)
			if mc and mc.model_import_strategy in ('full', 'create'):
				# process types we might create later
				continue

			matchingStrategy = mc.matching_strategy if mc else 'odooName'
			self._log('debug', "trying to map {} leaf objects of type {} with strategy {}".format(len(perModel), modelName, matchingStrategy))
			_logger.info("ids = {}".format(perModel))
			if matchingStrategy == 'odooName':
				if not modelName in allRemoteFields:
					allRemoteFields[modelName] = remoteOdoo.modelCall(modelName, 'fields_get')
				self._resolveByName(
					remoteOdoo, modelName, perModel, allRemoteFields[modelName]
				)
			elif matchingStrategy == 'explicitKeys':
				pass
			else:
				raise UserError("Unsupported matching strategy '{}' for model type {}".format(matchingStrategy, modelName))

		# return odoo connection
		return remoteOdoo

	def _resolveByName(self, remoteOdoo, modelName, remoteIds, remoteFields):
		fieldsToRead = ['display_name', 'id']
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

