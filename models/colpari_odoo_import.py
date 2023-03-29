# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
import odoo.addons.decimal_precision as dp

import logging

_logger = logging.getLogger(__name__)


class colpariOdooImportSource(models.Model):
	_name = 'odoo_import_source'
	_description = 'Odoo instance to import data from'
	_help = 'Access details for an odoo instance to import data from'



class colpariOdooImport(models.Model):
	_name = 'odoo_import_config'
	_description = 'Import configuration'
	_help = 'A configuration to import data from another odoo instance'

	name = fields.Char(required=True, index=True, string='Name', help='Display name')

	model_configs = fields.One2many('colpari.odoo_import_modelconfig', 'import_config')


	def execute(self):
		self.ensure_one()

		# models check
			# for model_configs
				# read remote model
				# warn about locally missing fields
					# for non-ignored field:
						# fail if locally required and not configured

		# import
			# build dependency graph
			# for graph.leafes
				# import or die





class colpariOdooImportModelConfig(models.Model):
	_name = 'odoo_import_modelconfig'
	_description = 'Import configuration for a certain model'

	import_config = fields.Many2one('colpari.odoo_import_config', required=True, ondelete='cascade')

	ir_model = fields.Many2one('ir.model', required=True, ondelete='cascade')

	identity_by_odoo_name = field.Boolean()

	key_fields = fields.Many2many('ir.model.fields')

	ignore_fields = fields.Many2many('ir.model.fields')

	strategy = fields.Selection([('ignore', 'Ignore'), ('create', 'Create if not found'), ('update', 'Update if existing, or create')])



class colpariOdooImportRun(models.Model):
	_name = 'odoo_import_run'
	_description = 'Import run'
	_help = 'A run of a specific import configuration'

	import_config = fields.Many2one('colpari.odoo_import_config', required=True, ondelete='cascade')

	state = fields.Selection([('configure', 'Configure'), ('runnable', 'Runnable'), ('running', 'Running'), ('finished', 'Finished'), ('failed', 'Failed')], default='configure')


	def _checkRunnable(self):
		'''
			connect to remote and verify models. move to state runnable if successful.
			for efficiency reasons we return the remote connection we have to make anyways, since we might be part of a _run()
		'''
		self.ensure_one()
		# create connection
		# for my config's model_configs
			# read remote model
			# for local field:
				# if locally required:
					# fail if missing on remote
					# fail if ignored
				# if object-type and not ignored:
					# fail if not configured

		# return odoo connection


	def _prepare(self):
		'''
			move to state runnable if _checkRunnable()
		'''
		self.ensure_one()
		remoteConnection = self._checkRunnable():
		if odooConnection:
			self.state = 'runnable'
		return remoteConnection

	def _run(self):
		''' _prepare() if runnable, read remote data '''
		self.ensure_one()
		
		odooConnection = self._prepare()

		if not odooConnection:
			return None
			# FIXME: raise something?


		# theorectically looks good
		# build local dependency graph
		# for graph.leafes
			# import or die

