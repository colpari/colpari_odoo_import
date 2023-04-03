# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
import odoo.addons.decimal_precision as dp

import logging
import xmlrpc.client

_logger = logging.getLogger(__name__)

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
	_order = "level, text"
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
		self.messages.create([{'level':level, 'text':text, 'import_run':self.id}])

	def _checkRunnable(self):
		'''
			connect to remote and verify models. move to state runnable if successful.
			for efficiency reasons we return the remote connection we have to make anyways, since we might be part of a _run()
		'''
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
		CC = self.import_config._getCC() # indexed config cache
		for modelConfig in modelsToImport:
			modelName = modelConfig.import_model.model
			# read remote model
			remoteFields = remoteOdoo.modelCall(modelName, 'fields_get')
			localFields = self.env[modelName].fields_get()
			msg = "checking {}/{} local/remote fields for type {}".format(len(localFields), len(remoteFields), modelName)
			_logger.info(msg)
			_logger.info(localFields)
			self._log('debug', msg)

			# for local field:
			for localFieldName, localField in localFields.items():
				fc = CC.getFieldConfig(modelName, localFieldName)
				_logger.info("checking {}/{} = {}".format(modelName, localFieldName, fc))
				if localField['required']:
					_logger.info("checking required {}/{} = {}".format(modelName, localFieldName, fc))
					# fail if missing on remote
					if localFieldName not in remoteFields:
						#TODO: provide default value in import config
						raise Exception("Field {}.{} is required but not found on remote side".format(modelName, localFieldName))
					if fc and fc.ignore:
						raise Exception("Field {}.{} is required ignored in import configuration".format(modelName, localFieldName))
				# if object-type and not ignored:
					# fail if not configured
			modelsProcessed+=1
			self.progress2 = int(
				5 + ((modelsProcessed/modelsToImportCount)*95)
			)
		# return odoo connection
		return remoteOdoo

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
			print(e)
			self._log('error', str(e))
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

