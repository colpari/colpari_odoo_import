# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError

import logging
import xmlrpc.client
import traceback

from .import_model_handler import ImportModelHandler, ImportException

_logger = logging.getLogger("colpari_odoo_import")
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
		#_logger.info("{}.modelCall({}, {}, #{}, {})".format(self, modelName, methodName, args and len(args[0]) or '<>', kwargs))
		#_logger.info("{}.modelCall({}, {}, #{}, {})".format(self, modelName, methodName, args, kwargs))
		return self._models.execute_kw(self._dbName, self._uid, self._password, modelName, methodName, args, kwargs)

	def getFieldsOfModel(self, modelName):
		if modelName not in self._fieldInfo:
			self._fieldInfo[modelName] = self.modelCall(modelName, 'fields_get')
		return self._fieldInfo[modelName]

	def __fieldNamesToRemote(self, handler, fieldsToRead):
		if handler.fieldNamesL2R:
			return [ handler.fieldNamesL2R.get(fn, fn) for fn in fieldsToRead ]
		else:
			return list(fieldsToRead)

	def readData(self, handler, fieldsToRead, searchDomain, _logFn, ids = None):
		''' read all objects or specific ids of a type '''
		if ids != None:
			if not ids:
				raise Exception("Empty id set")
			result = self.modelCall(handler.modelName, 'read', list(ids), fields = self.__fieldNamesToRemote(handler, fieldsToRead))
			# _logger.info("readData() : read {} remote {} records with {} ids given and {} fields".format(
			# 		len(result), handler.modelName, len(ids or []), len(fieldsToRead)
			# ))
		else:
			result = self.modelCall(handler.modelName, 'search_read', searchDomain, fields = self.__fieldNamesToRemote(handler, fieldsToRead))
			# _logger.info("readData() : read {} remote {} records with {} ids given and {} fields, domain={}".format(
			# 		len(result), handler.modelName, len(ids or []), len(fieldsToRead), searchDomain
			# ))

		# #_logFn('3_debug',
		# _logger.info("readData() : read {} remote {} records with {} ids given and {} fields".format(
		# 		len(result), handler.modelName, len(ids or []), len(fieldsToRead)
		# 	)#, modelName=handler.modelName
		# )

		if handler.fieldNamesR2L:
			for record in result:
				for remoteName, localName in handler.fieldNamesR2L.items():
					if remoteName in record:
						record[localName] = record.pop(remoteName)

		return result

class ImportContext():
	def __init__(self, importRun):
		self.env 			= importRun.env
		self.importRun 		= importRun
		self.log 			= importRun._log
		self.importConfig 	= importRun.import_config

		self._sharedDependencyWorkList = {
			# handler : {
			#	remoteId : {
			#		relatedType : set(relatedRemoteIds)
			#	}
			#}
		}

		connParams 			= self.importConfig.import_source
		self.remoteOdoo 	= OdooConnection( # make connection
			connParams.url, connParams.username, connParams.dbname, connParams.credential
		)

		self._handlers = {
			# modelName : ImportModelHandler
		}

		# create handlers for all configured models
		# 	(for unconfigured types we just create default handlers on-the-fly)
		# check if we have at least one stategy import handler with do_ivot
		haveOneImportPivotHandler = False
		for modelConfig in self.importConfig.model_configs:
			self.getHandler(modelConfig.import_model.model)
			if modelConfig.do_pivot and modelConfig.model_import_strategy == 'import':
				haveOneImportPivotHandler = True

		if not haveOneImportPivotHandler:
			raise ImportException("At least one model must have strategy 'import' and 'do_pivot' == True")

		# run checkConfig (NOTE: required, finishes handler setup and may create more handlers)
		for handler in list(self._handlers.values()):
			handler.checkConfig()

		#_logger.info("global time filter domain is: {}".format(self.importConfig.getTimeFilterDomain()))

	def getHandler(self, modelName):
		h = self._handlers.get(modelName)
		if not h:
			h = self._handlers[modelName] = ImportModelHandler(self, modelName)
		return h

	def hasHandler(self, modelName):
		return modelName in self._handlers

	def getConfiguredModelNames(self):
		return [
			config.import_model.model
				for config in self.importConfig.mapped('model_configs')
				if config.model_import_strategy != 'ignore'
		]
		#return self.importConfig.mapped('model_configs.import_model.model')

	def getConfiguredHandlers(self, *importStrategies):
		# return the handlers for all explicitly configured models
		_all = map(self.getHandler, self.getConfiguredModelNames())
		if importStrategies:
			return [ handler for handler in _all if handler.hasImportStrategy(*importStrategies) ]
		else:
			return _all

	def __logHandlerStatus(self, notInUI = False):

		#for handler in self.getConfiguredHandlers():
		for handler in self._handlers.values():
			#handler.updateResolvingStatus()
			if handler.hasImportStrategy('import', 'bulk') or handler.hasWork():
				if notInUI:
					_logger.info(handler.status())
				else:
					self.log('2_info', handler.status(), modelName=handler.modelName)

	def __crawl(self, dependencyIdsToResolve, phaseInformational):
		i = 0
		# read all dependency objects and maybe collect their dependencies. loop until no new dependencies are discovered
		while dependencyIdsToResolve:
			i+=1
			thisPass = dependencyIdsToResolve
			dependencyIdsToResolve = {}

			for handler, dependencyIds in thisPass.items():
				if not dependencyIds: # sanity check
					raise Exception("Empty dependency list")

				#_logger.info("{} crawl -> {}".format(handler.modelName, dependencyIds))

				if handler.hasImportStrategy('import', 'match'):
					handler.fetchRemoteKeys(dependencyIds)
					handler.resolveReadAndSchedule(dependencyIdsToResolve)
				elif handler.hasImportStrategy('bulk'):
					# records with strategy 'bulk' go straight to create, if they are not there already
					# this is needed because we call __crawl() also in the create phase
					# TODO: is this a lower level decision and should maybe be handled further down the call chain?
					newBulkIds = dependencyIds - handler.toCreate.keys() - handler.toUpdate.keys()
					if newBulkIds:
						handler.toCreate.update(
							handler._readRemoteDataAndCollectDepenencies(newBulkIds, dependencyIdsToResolve)
						)
						_logger.info("{} : added {} bulk records".format(handler.modelName, len(newBulkIds)))
				else:
					raise Exception("type {} phase {} : import strategy {} should not occur here".format(
						handler, phaseInformational, handler.importStrategy
					))

		# for handler in self.getConfiguredHandlers('import', 'match'):
		# 	handler._resolve()

		return i

	def _runReadTest(self):
		self.getHandler('account.move')._runReadTest()

	def run(self, onlyReadPhase):
		'''
			Tree shaking:
				- discover: main import types (id-less search of keys)
					- resolve -> (r,u,p)
					- (r?,u?) -> read data and schedule -> (2c, 2u, p)

				- collect dependencies for (2c, 2u) unless target type has 'bulk'
				- while dependencies and not numb
					- read dep keys
					- resolve dep keys and p -> (r,u,p)
					- if strategy == import
						- (r?,u?) -> read data and schedule -> (2c, 2u, p)

				- p=0
				- (2c, 2u) -> read related "bulk" types -> 2c

		'''
		mainHandlers = list(self.getConfiguredHandlers('import', 'match'))

		# first pass. fetch keys of import & match types, resolve and schedule
		dependencyIdsToResolve = {}

		for handler in mainHandlers:
			if handler.modelConfig.do_pivot:
				handler.fetchRemoteKeys(ids = None)

		for handler in mainHandlers:
			if handler.modelConfig.do_pivot:
				handler.resolveReadAndSchedule(dependencyIdsToResolve)

		_logger.info("phase 0 complete")

		iterations = self.__crawl(dependencyIdsToResolve, phaseInformational = 1)

		_logger.info("phase 1 finished after {} iterations".format(iterations))

		#TODO: provide todo-info logged by below line more prominent(ly?) in UI
		self.__logHandlerStatus()

		if onlyReadPhase:
			return True

		for IS_CREATE in (True, False): # first process objects to create, then update
			phaseName = 'create' if IS_CREATE else 'update'
			finishedAll = False
			_pass = 0
			while not finishedAll:
				_pass+=1
				finishedAll = True
				progressMade = False
				dependencyIdsToResolve = {}

				for handler in self.getConfiguredHandlers('import', 'bulk'):
					(finished, progess)  = handler.tryCreate(dependencyIdsToResolve) if IS_CREATE else handler.tryUpdate()
					finishedAll 		&= finished
					progressMade		|= progess

				if not finishedAll and not progressMade:
					self.__logHandlerStatus()
					self.log('0_error', "Nothing found writeable in {} pass #{}".format(phaseName, _pass))
					return False

				if dependencyIdsToResolve:
					# create phase may yield new dependencies and they in turn might yield new objects to create
					if not IS_CREATE:
						self.__logHandlerStatus(notInUI = True)
						raise Exception("There should be no new dependencies turning up in 'update' phase")
					# crawl dependencies
					self.__crawl(dependencyIdsToResolve, phaseInformational = 2)

					# do another turn
					finishedAll = False

				_logger.info("phase {} pass #{}, finished={}, progess={}".format(
					phaseName, _pass, finishedAll, progressMade
				))

				self.__logHandlerStatus(notInUI = True)

		return True


class colpariOdooImportRunMessage(models.Model):
	_name = 'colpari.odoo_import_run_message'
	_description = 'Import run diagnostic messages'
	_order = "write_date, model_name, level, text"
	_rec_name = 'text'

	import_run = fields.Many2one('colpari.odoo_import_run', required=True, ondelete='cascade')
	level = fields.Selection(
		[('0_error', 'Error'), ('1_warning', 'Warning'), ('2_info', 'Info'),('3_debug', 'Debug')],
		required=True
	)
	text = fields.Char()

	model_name = fields.Char()
	field_name = fields.Char()
	dependency_type = fields.Char()

	count_in_scope = fields.Integer()
	count_resolved = fields.Integer()
	count_2create = fields.Integer()
	count_2update = fields.Integer()

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

	def actionImportAsBulk(self):
		self.ensure_one()
		if not self.dependency_type:
			raise ValidationError("Action not applicable")

		self.import_run.import_config.setModelConfig(
			self.dependency_type, {'model_import_strategy' : 'bulk'}
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

	messages_debug = fields.One2many('colpari.odoo_import_run_message', 'import_run',  compute="_computeMessages")

	messages_non_debug = fields.One2many('colpari.odoo_import_run_message', 'import_run',  compute="_computeMessages")

	@api.depends('messages')
	def _computeMessages(self):
		for record in self:
			record.messages_debug = self.env['colpari.odoo_import_run_message']
			record.messages_non_debug = self.env['colpari.odoo_import_run_message']
			for m in record.messages:
				if m.level == '3_debug':
					record.messages_debug += m
				else:
					record.messages_non_debug += m

	def _log(self, level, text, modelName=False, fieldName=False, dependencyType=False, countInScope = False, cr = False):
		self.ensure_one()
		_logger.log(_logLevelMap[level], text)
		self.messages.create([{
			'level' : level, 'text' : text, 'import_run' : self.id,
			'model_name' : modelName, 'field_name' : fieldName, 'dependency_type' : dependencyType,

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

	def prepareRun(self):
		self._run(doNotWrite = True, onlyReadPhase = True)

	def testRun(self):
		self._run(doNotWrite = True, onlyReadPhase = False)

	def realRun(self):
		self._run(doNotWrite = False, onlyReadPhase = False)

	def _runReadTest(self):
		ImportContext(self)._runReadTest()

	def _run(self, doNotWrite, onlyReadPhase):
		self.ensure_one()
		self.messages.unlink()

		try:
			theImport = ImportContext(self)
			runResult = theImport.run(onlyReadPhase)
			if doNotWrite and not onlyReadPhase:
				savedMessages = self._copyMessages()
				self.env.cr.rollback()
				self.messages.unlink()
				self.messages.create(savedMessages)
			self.state = 'finished' if runResult else 'failed'
			self._log("2_info","============= {} (changes saved = {}) =============".format(self.state.upper(), not doNotWrite))

		except ImportException as ie:
			txt = str(ie)
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
