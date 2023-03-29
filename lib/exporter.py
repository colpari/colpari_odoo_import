# -*- coding: utf-8 -*-

from odoo import models, fields, api

import logging 
import json
import pickle

from datetime import date, datetime

_logger = logging.getLogger(__name__)

ColpariDependencies = [
	'project.project',
	'colpari.settlement',
	'colpari.activity_value_import_3cx',
	'res.bank',
	'res.partner',
	'res.partner.title',
	'res.partner.bank',
	'res.company',
	'res.currency',
	'res.users',
	'res.country',
	'res.country.state',
	'resource.resource',
	'res.users.log',
	'hr.employee',
	'auth.oauth.provider',
	'uom.uom',
	'product.product',
	'hr.skill.type',
	'hr.skill.level',
	'hr.skill',
	'hr.resume.line',
#	'hr.job',
#	'hr.department',
	'product.category',
	'purchase.order',
	'sale.order',
	'sale.order.line',
	'purchase.order',
	'purchase.order.line',
	'account.payment',
	'account.move',
	'account.move.line',
	'account.partial.reconcile',
	'account.fiscal.position',
	'account.fiscal.position.tax',
	'account.edi.document',
	'account.full.reconcile',
	'product.template',
	'mail.followers',
	'mail.channel',
	'mail.template',
	'mail.message',
	'mail.mail',
	'mail.notification',
	'mail.alias',
	'ir.attachment',
	'ir.mail_server',
	'ir.exports',
	'ir.exports.line',
	'fetchmail.server',
	'theme.ir.attachment',
	'gamification.goal',
	#'mail.tracking.value',
	#'website.visitor',
	#'website.track'
]

ColpariNameOptionally = [ # FIXME: export names explicitly for all exported types by default and decide on importing side
	'mail.template',
	'mail.alias',
	'mail.channel',
	'auth.oauth.provider',
	'account.fiscal.position',
	'account.fiscal.position.tax',
	'resource.resource',
	'product.product',
	'product.category',
	'res.country',
	'res.bank',
	'res.currency',
	'res.company',
	'res.users',
	'res.partner',
	'res.bank',
	'res.partner.title',
	'res.partner.bank',
	'res.country.state',
#	'hr.job',
#	'hr.department',
	'hr.employee',
	'colpari.action_type',
	'colpari.action_unit',
	'colpari.indicator',
	'colpari.feature',
	'colpari.feature.set',
	'ir.exports',
	'ir.exports.line',
	'uom.uom'
]

AllowedExportTypes = set([
	'str', 'int', 'float', 'bool', 'list', 'date', 'datetime', 'bytes'
])

# env['colpari.export']._export()
# for k in p.fields_get(): print("{} -> {}".format(k, p[k]))

# result dict:
#	'full'
# 		typeName-> sourceId -> data
# 	'nameRef'
# 		typeName
# 			{ sourceId : name }


# typeInfo:
#	'typeName'
#		'fields' -> odoo field info structure
#		'relFields' ->  {}

# odoo.addons.colpari_services.models.exporter.ColpariExport
# odoo.addons.colpari_services.models.exporter.ColpariImport
# e = odoo.addons.colpari_services.lib.exporter.ColpariExport(env) ; e.exportColpari('/tmp/out2.pickle')
# e = odoo.addons.colpari_services.lib.exporter.ColpariExport(env) ; e.exportColpari('/var/lib/odoo/filestore/out.pickle')
# i = odoo.addons.colpari_services.lib.importer.ColpariImport(env) ; i.doImport('/tmp/out2.pickle')
# i = odoo.addons.colpari_services.lib.importer.ColpariImport(env) ; i.doImportColpari('/var/lib/odoo/filestore/out.pickle', commit = True)
# 
class ColpariExport:

	def __init__(self, odooEnv):
		self.typeInfo = {}
		self.odooEnv = odooEnv
		self.typesExportFull = set()
		self.typesExportFullOnly = set()
		self.typeSelector = None
		self.typesToExport = set()
		self.result = {}
		self.leafsToExport = {}


	def getTypeInfo(self, typeName):
		return self.typeInfo.setdefault(typeName, {}).setdefault('fields', self.odooEnv[typeName].fields_get())

	def getRelatedModels(self, typeName):
		ti = self.getTypeInfo(typeName)
		return self.typeInfo.setdefault(typeName, {}).setdefault('relFields', { 
			fieldName : fieldDef['relation']
				for fieldName, fieldDef in ti.items()
					if 'relation' in fieldDef
		})

	def exportColpari(self, fileName = None, jsonMode = False): 	return self.export(
		ColpariDependencies, lambda name: name.startswith('colpari.'), {
			'product.product' : {
				'name' : { 'store' : True }
			},
			'res.partner' : {
				'property_supplier_payment_term_id' : { 'store' : True },
				'property_payment_term_id' 			: { 'store' : True },
				'property_account_position_id'		: { 'store' : True },
			},
		}, fileName, jsonMode = True
	)
	
	def exportTest(self, fieldOptionsOverride, fileName = None, jsonMode = False):
		return self.export(x, None, fieldOptionsOverride, fileName, jsonMode)


	def export(self, typesExportFull, typeSelector, fieldOptionsOverride, fileName = None, jsonMode = False):
		self.typesExportFull = set(typesExportFull) if typesExportFull else set()
		#self.typesOptionallyByName = set(typesOptionallyByName) if typesOptionallyByName else set()
		self.typeSelector = typeSelector
		self.typesToExport = {}
		self.result = {}
		# self.fieldsDebugged = {}
		self.leafsToExport = {}
		self.fieldOptionsOverride = fieldOptionsOverride if fieldOptionsOverride else {}

		# export full objects
		for typeName in self.odooEnv.keys():
			if self.shallExport(typeName):
				self._exportFull(typeName)
				self.result.setdefault('schema', {}).setdefault(typeName, self.getRelatedModels(typeName))
				if typeName in ColpariNameOptionally:
					typeInfo = self.getTypeInfo(typeName)
					self._exportLeaf(typeName,
						self.odooEnv[typeName].search([['active', 'in', [True,False]]] if 'active' in typeInfo else []).ids # get all
					)

		# export leaf objects references by name
		for typeName, ids in self.leafsToExport.items():
			self._exportLeaf(typeName, ids)

		_logger.info("saving output...")

		# write out
		if fileName:
			# always pickle it out
			with open(fileName, "wb") as output:
				pickle.dump(self.result, output)

			# maybe also dump json
			if jsonMode:
				jsonName = fileName + ".json"
				jsonText = json.dumps(self.result, default=self.serialize, indent=2)
				with open(jsonName, "w") as output:
					output.write(jsonText)
				_logger.info("wrote {} characters JSON to '{}'".format(len(jsonText), jsonName))

		# or return data
		else:
			#return self.result
			return json.dumps(self.result, default=self.serialize)


	def _exportFull(self, modelName):
		
		if 'full' in self.result and modelName in self.result['full']:
			# we already did this or are doing it currently
			return

		resultNode = {}

		theEnv = self.odooEnv[modelName]
		typeInfo = self.getTypeInfo(modelName)

		for record in theEnv.search([['active', 'in', [True,False]]] if 'active' in typeInfo else []):
			resultNode[record.id] = self.getObjDictFull(record)

		if resultNode:
			self.result.setdefault('full', {})[modelName] = resultNode
			numExported = len(resultNode)
			exportedKeys = list(resultNode.keys())
			_logger.info("FULL: {} -> {} [{}]".format(modelName, numExported, exportedKeys if numExported < 200 else exportedKeys[:200]))

	def _exportLeaf(self, modelName, ids):

		leafs = self.result.setdefault('nameRef', {}).setdefault(modelName, {})

		theEnv = self.odooEnv[modelName]

		for record in theEnv.browse(ids):
			leafs[record.id] = (
				record.display_name,
				# at least product.supplierinfo in odoo 14 returns a non-string name :-o
				record.name if 'name' in record and type(record.name) == type("") else None,
				record.get_external_id()[record.id]
			)

			for x in leafs[record.id]:
				if x != None and type(x).__name__ != 'str':
					raise Exception("Non string name in {}.{} : {}".format(modelName, record.id, leafs[record.id]))

		if leafs:
			_logger.info("LEAF: {} -> {}".format(modelName, len(leafs)))
			self.result['nameRef'][modelName] = leafs


	def shallExport(self, typeName):
		try: 				return self.typesToExport[typeName]
		except KeyError: 	return self.typesToExport.setdefault(typeName,
			typeName in self.typesExportFull or (
				(self.typeSelector and self.typeSelector(typeName)) and not self.odooEnv[typeName]._abstract
			)
		)

	def getObjDictFull(self, record):

		modelName = record._name
		typeInfo = self.getTypeInfo(modelName)
		refFields = self.getRelatedModels(modelName)
		result = {}

		for fieldName, fieldDef in typeInfo.items():
			if fieldName == "id": continue # no need to export that
			if fieldName in record and self.fieldOptionsOverride.setdefault(modelName, {}).setdefault(fieldName, {}).get('store', fieldDef.get('store')):
				if fieldName in refFields:
					relatedType = refFields[fieldName]
					# is a reference to another type, save the source ids
					result[fieldName] = record[fieldName].ids

					if not self.shallExport(relatedType):
						# that type is not to be exported fully - remember to export the used ids by name
						if relatedType not in self.leafsToExport:
							_logger.debug("DEPEND: {}.{} -> {}".format(modelName, fieldName, relatedType))
						self.leafsToExport.setdefault(relatedType, set()).update(record[fieldName].ids)

				else: # normal copy
					result[fieldName] = record[fieldName]

				t = type(result[fieldName])
				if t.__name__ not in AllowedExportTypes:
					raise Exception("Encountered unsupported field type {} in {}.{} : {}".format(t, modelName, fieldName, result[fieldName]))

				#if t.__name__ == 'list' and result[fieldName] and type(result[fieldName][0]).__name__ != 'int':
				#	_logger.info("LIST: {}.{} : {}".format(modelName, fieldName, result[fieldName]))

				# dbgName = record._name + "." + fieldName
				# if dbgName not in self.fieldsDebugged:
				# 	#_logger.info("T: {} -> {}".format(dbgName, result[fieldName]))
				# 	self.fieldsDebugged[dbgName] = 1


		#if modelName == 'mail.tracking.value': _logger.info("W: {}".format(result))

		return result

	def serialize(self, obj):
		'''default json serializer for unsupported types'''
		if isinstance(obj, (datetime, date)):
			# FIXME: use odoo.tools.misc.DEFAULT_SERVER_DATE_FORMAT and odoo.tools.misc.DEFAULT_SERVER_DATETIME_FORMAT for import-ready values
			return obj.isoformat()
		if isinstance(obj, bytes):
			return "[{} bytes]".format(len(obj))
		raise TypeError ("Dont know how to serialize {}".format(type(obj)))

