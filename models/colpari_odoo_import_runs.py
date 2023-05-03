# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

import logging
import xmlrpc.client
import traceback

from .import_model_handler import ImportModelHandler, ImportException

_logger = logging.getLogger(__name__)
_logLevelMap = {
	'3_debug' 	: logging.DEBUG,
	'2_info' 	: logging.INFO,
	'1_warning'	: logging.WARNING,
	'0_error' 	: logging.ERROR,
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
		#_logFn('3_debug',
		_logger.info("readData() : read {} remote {} records with {} ids given and {} fields".format(
				len(result), modelName, len(ids or []), len(fieldsToRead)
			)#, modelName=modelName
		)
		return result

class ImportContext():
	def __init__(self, importRun):
		self.env 			= importRun.env
		self.importRun 		= importRun
		self.log 			= importRun._log
		self.importConfig 	= importRun.import_config

		connParams 			= self.importConfig.import_source
		self.remoteOdoo 	= OdooConnection( # make connection
			connParams.url, connParams.username, connParams.dbname, connParams.credential
		)

		self._handlers = {
			# modelName : ImportModelHandler
		}

		# create handlers for all configured models
		# 	(for unconfigured types we just create default handlers on-the-fly)
		for modelConfig in self.importConfig.model_configs:
			self.getHandler(modelConfig.import_model.model)

		# run checkConfig (NOTE: required, finishes handler setup and may create more handlers)
		for handler in list(self._handlers.values()):
			handler.checkConfig()

	def getHandler(self, modelName):
		h = self._handlers.get(modelName)
		if not h:
			h = self._handlers[modelName] = ImportModelHandler(self, modelName)
		return h

	def getConfiguredModelNames(self):
		return [
			config.import_model.model
				for config in self.importConfig.mapped('model_configs')
				if config.model_import_strategy != 'ignore'
		]
		#return self.importConfig.mapped('model_configs.import_model.model')

	def getConfiguredHandlers(self):
		# return the handlers for all explicitly configured models
		return map(self.getHandler, self.getConfiguredModelNames())

	def __logHandlerStatus(self):

		#for handler in self.getConfiguredHandlers():
		for handler in self._handlers.values():
			if handler.hasContent():
				self.log('2_info', handler.status(), modelName=handler.modelName)

	def run(self):

		dependencyIdsToResolve = {
			# handler : set(ids)
		}

		configuredHandlers = list(self.getConfiguredHandlers())

		# first pass. fetch all import types and collect dependencies
		for handler in configuredHandlers:
			if handler.importStrategy != 'import':
				continue

 			#TODO: add remote consideration domain
			ids = handler.fetchRemoteKeys(ids = None)

			handler.readIncremental(ids, dependencyIdsToResolve)

		i = 0
		# read all dependency objects and maybe collect new dependencies. loop until no dependencies left
		while dependencyIdsToResolve:
			i+=1
			thisPass = dependencyIdsToResolve
			dependencyIdsToResolve = {}

			for handler, dependencyIds in thisPass.items():
				if not dependencyIds: # sanity check
					raise Exception("Empty dependency list")

				_logger.info("{} reading phase pass {} with {} ids".format(handler.modelName, i, len(dependencyIds)))

				handler.readIncremental(dependencyIds, dependencyIdsToResolve)

		_logger.info("reading phase finished after {} iterations".format(i))

		#TODO: provide todo-info logged by below line more prominent(ly?) in UI
		self.__logHandlerStatus()

		for IS_CREATE in (True, False): # first process objects to create, then update
			phaseName = 'create' if IS_CREATE else 'update'
			finished = False
			_pass = 0
			while not finished:
				_pass+=1
				finished = True
				handlersSucceeded = 0

				for handler in configuredHandlers:
					dataToProcess = (handler.toCreate if IS_CREATE else handler.toUpdate)
					if not dataToProcess:
						# nothing to do (anymore) for this type
						continue
					processedCount = handler.tryCreate() if IS_CREATE else handler.tryUpdate()
					if processedCount:
						# we created the objects of this type. progess! :)
						handlersSucceeded += 1
					if processedCount < len(dataToProcess):
						# but not all objects (yet)
						finished = False

				_logger.info("phase {} pass #{}, {}/{} handlers succeeded, finished={}".format(
					phaseName, _pass, handlersSucceeded, len(configuredHandlers), finished
				))

				if not handlersSucceeded:
					self.__logHandlerStatus()
					raise ValidationError("Nothing found writeable in {} pass #{}".format(phaseName, _pass))


class colpariOdooImportRunMessage(models.Model):
	_name = 'colpari.odoo_import_run_message'
	_description = 'Import run diagnostic messages'
	_order = "write_date, model_name, level, text"
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

	messages = fields.One2many('colpari.odoo_import_run_message', 'import_run', readonly=True)

	def _log(self, level, text, modelName=False, fieldName=False, dependencyType=False):
		self.ensure_one()
		_logger.log(_logLevelMap[level], text)
		self.messages.create([{
			'level' : level, 'text' : text, 'import_run' : self.id,
			'model_name' : modelName, 'field_name' : fieldName, 'dependency_type' : dependencyType
		}])

	def _copyMessages(self):
		''' copy objects from our 'messages' field to dictionaries so we can save them again after a rollback '''
		self.ensure_one()
		result = [
				{ fn : message[fn] for fn in self.messages._fields.keys() }
			for message in self.messages
		]
		#print(result)
		for message in result:
			message['import_run'] = message['import_run'].id

		return result

	def testRun(self):
		self._run(doNotWrite = True)

	def realRun(self):
		self._run(doNotWrite = False)

	def _run(self, doNotWrite):
		self.ensure_one()
		self.messages.unlink()

		try:
			theImport = ImportContext(self)
			theImport.run()
			if doNotWrite:
				savedMessages = self._copyMessages()
				self.env.cr.rollback()
				self.messages.unlink()
				self.messages.create(savedMessages)
			self.state = 'finished'
			self._log("2_info","============= SUCCESS (changes saved = {}) =============".format(not doNotWrite))

		except ImportException as ie:
			txt = traceback.format_exc()
			#self._log('0_error', str(ie), **ie.kwargs)
			#self._log('0_error', str(ie), **ie.kwargs)
			savedMessages = self._copyMessages()
			self.env.cr.rollback()
			self.messages.unlink()
			self.messages.create(savedMessages)
			self._log('0_error', txt, **ie.kwargs)
			self.state = 'failed'

		except Exception as e:
			txt = traceback.format_exc()
			savedMessages = self._copyMessages()
			self.env.cr.rollback()
			self.messages.unlink()
			self.messages.create(savedMessages)
			self._log('0_error', txt)
			self.state = 'failed'
