# -*- coding: utf-8 -*-


#from odoo import models, fields, api
import logging

_logger = logging.getLogger(__name__)

def fixExternalIds(self):
	if self._abstract:
		_logger.info("Not fixing external ids for abstract type {}/{}".format(self, self._name))
		return

	_logger.info("fixExternalIds {} / {}".format(self, self._name))
	modelData = self.env['ir.model.data']
	allOfMe = self.search([])
	for record in allOfMe:
		entry = modelData.search([
			['module', '=', 'colpari_services'],
			['model', '=', self._name],
			['res_id', '=', record.id]
		])
		expectedName = self._name.split('.')[1]+'_'+(record.name.casefold().replace(' ', ''))
		if not entry:
			continue
			# newEntry = {
			# 	'module' 	: 'colpari_services',
			# 	'model'		: self._name,
			# 	'res_id'	: record.id,
			# 	'name'		: expectedName
			# }
			# _logger.info("{} has no entry. creating: {}".format(record, newEntry))
			# self.env['ir.model.data'].create([newEntry])
		else:
			if '_' in entry.name:
				_logger.info("{} ok : '{}'".format(record, entry.name))
			else:
				_logger.info("{} not ok : '{}' -> '{}'".format(record, entry.name, expectedName))
				entry.name = expectedName

def fixExternalIds2(self, records):
	_logger.info("fixExternalIds {} / {}".format(self, self._name))
	modelData = self.env['ir.model.data']
	for record in records:
		entry = modelData.search([
			['module', '=', 'colpari_services'],
			['model', '=', self._name],
			['res_id', '=', record.id]
		])
		expectedName = self._name.split('.')[1]+'_'+(record.name.casefold().replace(' ', ''))
		if not entry:
			newEntry = {
				'module' 	: 'colpari_services',
				'model'		: self._name,
				'res_id'	: record.id,
				'name'		: expectedName
			}
			_logger.info("{} has no entry. creating: {}".format(record, newEntry))
			modelData.create([newEntry])
		else:
			if '_' in entry.name:
				_logger.info("{} ok : '{}'".format(record, entry.name))
			else:
				_logger.info("{} not ok : '{}' -> '{}'".format(record, entry.name, expectedName))
				entry.name = expectedName


####### Fixing code to run in odoo shell:
# from odoo.addons.colpari_services.lib.util import fixExternalIds2

# at = env['colpari.action_type']
# ats = at.search([['name', 'in', ['call', 'mail', 'chat', 'ticket', 'incident', 'work', 'disposition', 'training', 'meeting', 'coaching', 'planning', 'controlling']]])
# fixExternalIds2(at, ats)

# au = env['colpari.action_unit']
# aus = au.search([['name', 'in', ['minute', 'hour', 'day', 'month', 'created', 'responded', 'processed', 'closed']]])
# fixExternalIds2(au, aus)

# i = env['colpari.indicator']
# iis = i.search([['name', 'in', ['Activity quantity', 'CSAT', 'NPS', 'CPH', 'Activity count']]])
# fixExternalIds2(i, iis)

