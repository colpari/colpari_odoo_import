# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
import odoo.addons.decimal_precision as dp

import logging

_logger = logging.getLogger(__name__)


class colpariOdooImportSource(models.Model):
	_name = 'colpari.odoo_import_source'
	_description = 'Odoo instance to import data from'
	_help = 'Access details for an odoo instance to import data from'

	name 		= fields.Char()
	url 		= fields.Char()
	username 	= fields.Char()
	dbname 		= fields.Char()
	credential 	= fields.Char()

class ConfigCache():
	def __init__(self, importConfig):
		self.config = importConfig
		self._configCache = CC = {}
		_logger.info("{} building config cache".format(importConfig))
		for modelConfig in importConfig.model_configs:
			CC[modelConfig.import_model.model] = {
				'config' : modelConfig,
				'fields' : {
					fieldConfig.import_field.name : fieldConfig
						for fieldConfig in modelConfig.field_configs
				}
			}

	def getModelConfig(self, modelName):
		return self._configCache.setdefault(modelName, {})

	def getFieldConfig(self, modelName, fieldName):
		return self.getModelConfig(modelName).setdefault('fields', {}).setdefault(fieldName, {})

	def shallImportField(self, modelName, fieldName):
		#TODO: complete
		return self.getFieldConfig(modelName, fieldName)


class colpariOdooImport(models.Model):
	_name = 'colpari.odoo_import_config'
	_description = 'Import configuration'
	_help = 'A configuration to import data from another odoo instance'

	name = fields.Char(required=True, index=True, string='Name', help='Display name')

	model_configs = fields.One2many('colpari.odoo_import_modelconfig', 'import_config')

	import_source = fields.Many2one('colpari.odoo_import_source', required=True, ondelete='restrict')

	def _getCC(self):
		self.ensure_one()
		return ConfigCache(self)


class colpariOdooImportModelConfig(models.Model):
	_name = 'colpari.odoo_import_modelconfig'
	_description = 'Import configuration for a certain model'

	_sql_constraints = [(
		'model_config_uniq', 'unique(import_config, import_model)',
		'Multiple configurations for the same model in one import configuration are not allowed'
	)]

	name = fields.Char(compute="_computeName")
	def _computeName(self):
		for record in self:
			record.name = record.import_model and record.import_model.name or ''

	import_config = fields.Many2one('colpari.odoo_import_config', required=True, ondelete='cascade')

	import_model = fields.Many2one('ir.model', required=True, ondelete='cascade')

	model_import_strategy = fields.Selection([
		('ignore'		, 'Ignore'),
		('full'			, 'Create or update'),
		('create'		, 'Create only (if not found)'),
		('updateOnly'	, 'Update if existing'),
	], default='full', required=True)

	matching_strategy = fields.Selection([
		('odooName'		, 'Match by odoo name'),
		('explicitKeys'	, 'Match by configured key fields')
	], default='odooName', required=True)

	field_configs = fields.One2many('colpari.odoo_import_fieldconfig', 'model_config')


class colpariOdooImportFieldConfig(models.Model):
	_name = 'colpari.odoo_import_fieldconfig'
	_description = 'Import configuration for a certain model field'

	_sql_constraints = [(
		'field_config_uniq', 'unique(model_config, import_field)',
		'Multiple configurations for the same field in one import model configuration are not allowed'
	)]

	name = fields.Char(compute="_computeName")
	def _computeName(self):
		for record in self:
			record.name = record.import_field and record.import_field.name or ''

	model_config = fields.Many2one('colpari.odoo_import_modelconfig', required=True, ondelete='cascade')

	model_id = fields.Many2one('ir.model', related='model_config.import_model')
	import_field = fields.Many2one('ir.model.fields', required=True, ondelete='cascade', domain="[('model_id', '=', model_id)]")

	field_import_strategy = fields.Selection([
		('literal'		, 'Literal value'),
		('ignore'		, 'Ignore field'),
		('explicitKey'	, 'Part of the explicit matching key')
	], default='full', required=True)

	value_mappings = fields.One2many('colpari.odoo_import_fieldmapping', 'field_config')



class colpariOdooImportFieldMapping(models.Model):
	_name = 'colpari.odoo_import_fieldmapping'
	_description = 'Value mapping for importing a certain model field'


	field_config = fields.Many2one('colpari.odoo_import_fieldconfig')

	local_value = fields.Char(required=True)

	remote_value = fields.Char(required=True)

