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


class colpariOdooImport(models.Model):
	_name = 'colpari.odoo_import_config'
	_description = 'Import configuration'
	_help = 'A configuration to import data from another odoo instance'

	name = fields.Char(required=True, index=True, string='Name', help='Display name')

	model_configs = fields.One2many('colpari.odoo_import_modelconfig', 'import_config')

	import_source = fields.Many2one('colpari.odoo_import_source', required=True, ondelete='restrict')

	only_required_dependencies = fields.Boolean(string="Ignore dependencies which are not required", default=True)


class colpariOdooImportModelConfig(models.Model):
	_name = 'colpari.odoo_import_modelconfig'
	_description = 'Import configuration for a certain model'

	_order = 'model_import_strategy DESC'
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

	import_model_name = fields.Char(related='import_model.model')

	do_create = fields.Boolean(string="Create objects if not found locally", default=True)
	do_update = fields.Boolean(string="Update objects if found locally", default=True)

	only_required_dependencies = fields.Boolean(string="Ignore dependencies which are not required", default=False)

	#TODO: add remote consideration domain

	model_import_strategy = fields.Selection([
		('import'	, 'Create or update'),
		('match'	, 'Do not import but explicitly configure matching'),
		('ignore'	, 'Ignore (do not import or match)'),
	], default='import', required=True)

	matching_strategy = fields.Selection([
		('odooName'		, 'Match by odoo name'),
		('explicitKeys'	, 'Match by configured key fields')
	], default='odooName', required=True)

	field_configs = fields.One2many('colpari.odoo_import_fieldconfig', 'model_config')

	def getConfiguredKeyFields(self):
		self.ensure_one()
		return self.field_configs.filtered(lambda fc : fc.field_import_strategy == 'key')

	def getConfiguredKeyFieldNames(self):
		return set(self.getConfiguredKeyFields().mapped('import_field.name'))


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
		('import'	, 'Import the value'),
		('ignore'	, 'Ignore field'),
		('key'		, 'Use as (part of) custom key')
	], default='import', required=True)

	value_mappings = fields.One2many('colpari.odoo_import_fieldmapping', 'field_config')

	def mapsToDefaultValue(self):
		''' returns the local value to map to iff there is exactly one mapping with an empty remote value '''
		self.ensure_one()
		if len(self.value_mappings) == 1 and not self.value_mappings[0].remote_value:
			return self.value_mappings[0].local_value
		else:
			return None



class colpariOdooImportFieldMapping(models.Model):
	_name = 'colpari.odoo_import_fieldmapping'
	_description = 'Value mapping for importing a certain model field'

	_sql_constraints = [(
		'remote_value_uniq', 'unique(field_config, remote_value)',
		'Remote value must be unique per field in field value mappings'
	)]

	field_config = fields.Many2one('colpari.odoo_import_fieldconfig', readonly=True)

	local_value = fields.Char(required=True)

	remote_value = fields.Char(required=False)

	name = fields.Char(compute='_computeName')
	def _computeName(self):
		for record in self:
			record.name = (
					("'{}' ".format(record.remote_value) if record.remote_value else '')
				+	"=> '{}'".format(record.local_value)
			)

