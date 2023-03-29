# -*- coding: utf-8 -*-

from odoo import models, fields, api

import logging 
import json
import pickle

from datetime import date, datetime

_logger = logging.getLogger(__name__)


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

# odoo.addons.colpari_services.lib.models.exporter.ColpariExport
# odoo.addons.colpari_services.lib.models.exporter.ColpariImport
# e = odoo.addons.colpari_services.lib.exporter.ColpariExport(env) ; e.exportColpari('/tmp/out2.pickle')
# e = odoo.addons.colpari_services.lib.exporter.ColpariExport(env) ; e.exportColpari('/var/lib/odoo/filestore/out.pickle')
# i = odoo.addons.colpari_services.lib.importer.ColpariImport(env) ; i.doImport('/tmp/out2.pickle')
# i = odoo.addons.colpari_services.lib.importer.ColpariImport(env) ; i.doImportColpari('/var/lib/odoo/filestore/out.pickle', commit = True)

class ColpariImport:

	def __init__(self, odooEnv):
		self.typeInfo = {}
		self.odooEnv = odooEnv
		self.inputData = None
		self.idMaps = {}
		self.clearRepeatedLogMessages()

	def _debug(self, msg):
		if msg not in self.logMessagesSeen:
			_logger.debug(msg)
			self.logMessagesSeen.add(msg)

	def _info(self, msg):
		if msg not in self.logMessagesSeen:
			_logger.info(msg)
			self.logMessagesSeen.add(msg)

	def _warn(self, msg):
		if msg not in self.logMessagesSeen:
			_logger.warning(msg)
			self.logMessagesSeen.add(msg)

	def clearRepeatedLogMessages(self):
		self.logMessagesSeen = set()

	def getTypeInfo(self, typeName):
		try:
			return self.typeInfo.setdefault(typeName, {}).setdefault('fields', self.odooEnv[typeName].fields_get())
		except KeyError:
			raise Exception("Type {} not found in system".format(typeName))


	def getRelatedModels(self, typeName):
		ti = self.getTypeInfo(typeName)
		return self.typeInfo.setdefault(typeName, {}).setdefault('relFields', { 
			fieldName : fieldDef['relation']
				for fieldName, fieldDef in ti.items()
					if 'relation' in fieldDef
		})


	def __globalValueMapping(self, typeName, fieldName, value):
		''' applied to all imported fields  '''
		if value and (type(value) == type("")) and 'o_services' in value:
			self._info("replacing 'o_services' in '{}'".format(value))
			return value.replace("o_services", "colpari_services")

		return value


	def doImportColpari(self, fileName, jsonMode = False, commit = False):
		return self.doImport(fileName, {
			'mail.template'		: { '$updateExisting' : True },
			'res.company'		: { '$updateExisting' : True },
			'ir.attachment'		: { '$notByName' : True },
#			'colpari.action_price_rule'		: { '$extraDeps' : ['product.product', 'product.category'] },
			'mail.mail'			: { '$truncate' : True },
#			'mail.alias'		: { '$truncate' : False, '$extraDeps' : ['hr.job'] },
			'mail.message'		: { '$truncate' : True },
			'mail.notification'	: { '$truncate' : True },
			'ir.actions.act_window' : { '$truncate' : True },
#			'mail.notification' : {
#				'failure_type' : {
#					'valueMapping' : { 
#						'SMTP' 		: 'mail_smtp',
#						'UNKNOWN'	: 'unknown',
#						'RECIPIENT'	: 'mail_email_invalid'
#					}
#				}
#			},
			'account.move' : {
				'state' : {
					'valueMapping' : {
						'posted' 	: 'draft',
					}
				}
			},
			'ir.mail_server' : {
				'active' : { # deactivate mail servers on import
					'valueMapping' : {
						True 	: False,
					}
				}
			},
			'fetchmail.server' : {
				'active' : { # deactivate mail servers on import
					'valueMapping' : {
						True 	: False,
					}
				}
			},
			
			'gamification.goal' 			: { '$truncate' : True },
			'mail.followers' 				: { '$truncate' : True },
			'mail.tracking.value' 			: { '$truncate' : True },
			'mail.channel.partner' 			: { '$truncate' : True },
#			'ir.model.fields' 				: { '$truncate' : True },
#			'product.supplierinfo'			: { '$truncate' : True },
#			'account.move'					: { '$truncate' : True },
#			'account.move.line'				: { '$truncate' : True },
#			'account.fiscal.position'		: { '$truncate' : True },
#			'account.fiscal.position.tax'	: { '$truncate' : True },
			'account.payment'				: { '$truncate' : True },
			'account.edi.document'			: { '$truncate' : True },
			'account.partial.reconcile'		: { '$truncate' : True },
			'account.full.reconcile'		: { '$truncate' : True },
			'website.visitor'				: { '$truncate' : True },
			'website.track'					: { '$truncate' : True },

			'sale.order' : {
				'message_follower_ids' : {
					'valueMapping' : lambda c,t,f,v: [] # drop that shit
				}
			},
			'account.move.line' : {
				#'price_subtotal' : {
				#	'valueMapping' : lambda c,t,f,v: c._info("aml.ps = {}".format(v)) or v
				#}
			},
			'sale.order.line' : {
				'product_id' : {
					'required' : True
				}
			},
			'purchase.order.line' : {
				'product_id' : {
					'required' : True
				}
			},
			'product.product' : {
				'product_tmpl_id' : {
					'required' : False
				}
			},
		}, jsonMode, commit)

	def doImport(self, fileName, fieldMapping, jsonMode = False, commit = False):
		try:				return self._doImport(fileName, fieldMapping, jsonMode, commit)
		except Exception as e:
			self._info("rolling back current transaction...")
			self.odooEnv.cr.rollback()
			raise e

	def _queueUnresolvedIdsForImport(self):
		''' all objects which are not resolved by name at this point go to stage 0, generation 0 '''
		s0 = self.stage0[0] = {} #TODO: trasform the following code to a dict comprehension 
		for typeName, perType in self.inputData['full'].items():
			if self._truncateType(typeName): continue # we shall ignore this completely			
			if typeName not in self.idMaps or self._doUpdateTypeIfPresent(typeName):
				# nothing mapped by name yet or existing objects should be updated -> import/consider all
				s0[typeName] = perType
			else:
				# check for each id
				for oldId, oldRecord in perType.items():
					if typeName not in self.idMaps or oldId not in self.idMaps[typeName]:
						s0.setdefault(typeName, {})[oldId] = oldRecord

	def _doImport(self, fileName, fieldMapping, jsonMode = False, commit = False):
		self.inputData = None
		self.idMaps = {}
		self.ignoreFields = {}
		self.stage0 = {}
		self.stage1 = {}
		self.fieldMapping = fieldMapping if fieldMapping else {}
		self.clearRepeatedLogMessages()

		if jsonMode:
			with open(fileName, "r") as input:
				self.inputData = json.load(input)
		else:
			with open(fileName, "rb") as input:
				self.inputData = pickle.load(input)

		self._resolveLeafs()
		self._queueUnresolvedIdsForImport()

		generationNumber = 0
		done = False

		while not done:
			self.clearRepeatedLogMessages()
			initialGenerationsize = len(self.stage0[generationNumber])
			self._info("stage 0, gen {}, {} types".format(generationNumber, initialGenerationsize))
			for typeName in list(self.stage0[generationNumber].keys()): # copy key set since we modify it in the loop
				self._importType(typeName, generationNumber)

			if len(self.stage0[generationNumber]) > 0:
				raise Exception(
					"Current generation ({}) should be empty after one complete loop. Remaining entries are: {}"
						.format(generationNumber, list(self.stage0[generationNumber].keys()))
					)

			self._info("stage 0, gen {} DONE".format(generationNumber))

			generationNumber += 1
			nextGenerationsize = len(self.stage0.get(generationNumber,[]))

			if nextGenerationsize == 0:
				done = True
			elif nextGenerationsize >= initialGenerationsize:
				#raise Exception("No additional type ({}/{}) could be saved in generation {} - aborting.\n\nleftover: {}\n\nIDMaps: {}".format(initialGenerationsize, nextGenerationsize, generationNumber-1, self.stage0[generationNumber].keys(), self.idMaps))
				raise Exception("No additional type ({}/{}) could be saved in generation {} - aborting.\n\nleftover: {}".format(initialGenerationsize, nextGenerationsize, generationNumber-1, list(self.stage0[generationNumber].keys())))


		self._info("stage 0 done after {} iterations".format(generationNumber))
		
		self.clearRepeatedLogMessages()

		#self._info("BACKLOG: {}".format(list(self.stage1.keys())))

		self._createNonRequiredRelations()

		# for relatedType, perType in self.mappingBacklog.items():
		# 	self._info("BL: {} -> {}".format(relatedType, list(perType.keys())))

		if commit:
			self._info("committing...")
			self.odooEnv.cr.commit()
		else:
			self._info("rolling back...")
			self.odooEnv.cr.rollback()


	def _truncateType(self, typeName):
		return self.fieldMapping.setdefault(typeName, {}).setdefault('$truncate', False)

	def _doUpdateTypeIfPresent(self, typeName):
		return self.fieldMapping.setdefault(typeName, {}).setdefault('$updateExisting', False)

	def _getExtraDeps(self, typeName):
		return self.fieldMapping.setdefault(typeName, {}).setdefault('$extraDeps', [])


	def _mapFieldValue(self, typeName, fieldName, value):
		fieldMapping = self.fieldMapping.setdefault(typeName, {}).setdefault(fieldName, {}).get('valueMapping')
		if fieldMapping:
			if callable(fieldMapping):
				return fieldMapping(self, typeName, fieldName, value)
			else:
				try: 				return fieldMapping.get(value, value)
				except KeyError:	raise Exception("unable to map {}.{} value {}".format(typeName, fieldName, value))

		return self.__globalValueMapping(typeName, fieldName, value)


	def _postProcess(self, typeName, newRecord):
		pp = self.fieldMapping.setdefault(typeName, {}).get('$postProcess', None)
		if pp:
			return pp(self, typeName, newRecord)

		return newRecord


	def _adjustRelationFieldCardinality(self, typeName, fieldName, value):
		fieldInfo = self.getTypeInfo(typeName)[fieldName]
		if fieldInfo['type'] == 'many2one':
			if len(value) > 1:
				raise Exception("More than one id mapped in many2one field {}.{}@{}".format(fullType, field, oldId))
			if len(value) == 1:
				return value[0]
			else:
				return None

		return value


	def _importType(self, fullType, generationNumber):

		oldRecords = self.stage0[generationNumber].pop(fullType, None)

		if oldRecords == None:
			return None

		if len(oldRecords) < 1: # nothing to do/map
			return self.idMaps.setdefault(fullType, {})

		self._info("try: {} x {}".format(len(oldRecords), fullType))

		# check if import relations comply with current schema
		relFieldsInput = self.inputData['schema'][fullType]
		relFieldsCurrent = self.getRelatedModels(fullType)
		typeInfo = self.getTypeInfo(fullType)

		for field in typeInfo.keys():
			
			ignoreFields = self.ignoreFields.setdefault(fullType, set())

			if field in ignoreFields:
				continue # ignored

			relType = relFieldsInput.get(field)

			if relType:
				if field not in typeInfo:
					# field was removed 
					if field not in ignoreFields:
						self._warn("{}.{} in input data does not exist in current schema - ignoring".format(fullType, field))
						ignoreFields.add(field)
					continue 
			
				if field not in relFieldsCurrent:	
					if field not in ignoreFields:
						self._warn("{}.{} in input data schema points to type {} but in the current schema the field is not relational ({})".format(fullType, field, relType, typeInfo[field]))
						ignoreFields.add(field)
			
				elif relFieldsCurrent[field] != relType:
					if field not in ignoreFields:
						self._warn("{}.{} in input data schema points to type {} but points to type {} in the current schema".format(fullType, field, relType, relFieldsCurrent[field]))
						ignoreFields.add(field)

		# assure all extra dependencies are imported
		for dependency in self._getExtraDeps(fullType):
			if not self._importType(dependency, generationNumber):
				self._info("cannot write write {} yet (missing extra dep {})".format(fullType, dependency))
				return None


		toInsert = []
		stage1Entry = {}
		newIdMapping = {}
		toUpdate = {}
		existingIdMapping = self.idMaps.get(fullType) or {} # some might be mapped by name and we maybe want to update their fields

		ignoreFields = self.ignoreFields.setdefault(fullType, set())

		for oldId, oldRecord in oldRecords.items():
			
			#if fullType == 'mail.tracking.value': self._info("R {} : {}".format(fullType, oldRecord))

			newRecord = dict(oldRecord)
			
			for field, value in list(newRecord.items()): # copy since we modify the dict in this loop

				if field not in typeInfo:
					if field not in ignoreFields:
						self._info("ignoring {}.{} since it does not exist".format(fullType, field))
					ignoreFields.add(field)

				if field in ignoreFields:
					del newRecord[field]
					continue # field is ignored or was removed

				value = newRecord[field] = self._mapFieldValue(fullType, field, value)

				if field in relFieldsCurrent and value: # map non-empty relational fields
					relatedType = relFieldsCurrent[field]
					mappingResult = self._mapIds(relatedType, value, generationNumber)
					if mappingResult == None:
						# unable to map yet...
						if self.fieldMapping.setdefault(fullType, {}).setdefault(field, {}).setdefault('required', typeInfo[field].get('required')):
							# ...and not able to save without. put back one generation higher and abort
							self.stage0.setdefault(generationNumber+1, {})[fullType] = oldRecords
							#self._info("cannot write write {}.{} ({} missing) (idm: {})".format(fullType, field, relatedType, self.idMaps))
							self._info("cannot write write {}.{} yet (missing {})".format(fullType, field, relatedType))
							return None
						# remember unmapped field and continue with empty
						stage1Entry.setdefault('backlog', {}).setdefault(relatedType, {}).setdefault(oldId, {})[field] = value
						self._debug("delaying {}.{} (missing {})".format(fullType, field, relatedType))
						mappingResult = []

					mappingResult = self._adjustRelationFieldCardinality(fullType, field, mappingResult)

					newRecord[field] = mappingResult

			newRecord2 = self._postProcess(fullType, newRecord)
			if newRecord2:
				# decide if we insert or update
				if self._doUpdateTypeIfPresent(fullType) and oldId in existingIdMapping:
					toUpdate[existingIdMapping[oldId]] = newRecord2
				else:
					toInsert.append( (oldId, newRecord2) )
			else:
				self._info("{} was dropped by postprocess: ({})".format(fullType, newRecord))


		# update existing records
		updatedRecords = []
		if toUpdate:
			try:
				self._info("updating {} {} objects".format(len(toUpdate), fullType))			
				for existingId, newData in toUpdate.items():
					r = self.odooEnv[fullType].browse(existingId)
					r.write(newData)
					updatedRecords.append(r)
			except Exception as e:
				self._info("Error updating the following {} records: ({})".format(fullType, str(e)))
				for x in toUpdate.items(): self._info("E: {} {}".format(fullType, x))
				raise e
		

		# insert new records
		try:
			createdRecords = self.odooEnv[fullType].create([x[1] for x in toInsert])
		except Exception as e:
			self._info("Error writing the following {} records: ({})".format(fullType, str(e)))
			for x in toInsert: self._info("E: {} {}".format(fullType, x))
			raise e
			#return None


		if len(createdRecords) != len(toInsert):
			raise Exception("createdRecords ({})) != toInsert ({})".format(len(createdRecords), len(toInsert)))

		i = 0 # create new id map
		for createdRecord in createdRecords:
			newId = createdRecord.id
			oldId = toInsert[i][0]
			newIdMapping[oldId] = newId
			i+=1

		self.idMaps.setdefault(fullType, {}).update(newIdMapping)

		result = self.idMaps[fullType]

		# DONE

		# update possible backlog entry
		if stage1Entry: 
			self._info("partially wrote {} ({})".format(fullType, result if len(result) < 11 else len(result)))
			# save new records by new id
			stage1Entry['records'] = { r.id : r for r in list(createdRecords)+updatedRecords } # merge updated records as if they are new
			# renumber backlog dictionary by new ids
			stage1Entry['backlog'] = {
				relatedType : {
					newIdMapping[oldId] if oldId in newIdMapping else existingIdMapping[oldId] : entry # merge updated records as if they are new
					for oldId, entry in blE.items()
				}
				for relatedType, blE in stage1Entry['backlog'].items()

			}

			self.stage1[fullType] = stage1Entry

		else:
			self._info("fully wrote {} {} objects".format(fullType, len(toInsert)))

		return result


	def _createNonRequiredRelations(self):

		for typeName, perType in self.stage1.items():
			if self._truncateType(typeName): continue
			#if 'backlog' not in perType:
			#	self._warn("Found stage 1 entry without backlog for type {}".format(typeName))
			#	continue
			recordDict = perType['records']
			updateDict = {}
			typeInfo = self.getTypeInfo(typeName)

			for relatedType, perRelatedType in perType['backlog'].items():

				if self._truncateType(relatedType): continue

				self._info("connecting {} -> {}".format(
					typeName, relatedType
				))

				for _id, fieldDict in perRelatedType.items():
					newRecord = recordDict[_id]
					recordUpdate = updateDict.setdefault(_id, {})
					for fieldName, oldRelationalValues in fieldDict.items():
						mappedIds = self._mapIds(relatedType, oldRelationalValues)
						# mappedObjects = self.odooEnv[relatedType].browse(mappedIds)
						# if len(mappedObjects) != len(oldRelationalValues):
						# 	raise Exception("Incorrect number of mapped {} objects ({}) for mapped id ({}) from old value ({}) for {}.{}".format(
						# 		relatedType, mappedObjects, mappedIds, oldRelationalValues, typeName, fieldName)
						# 	)
						# self._info("{}.{} -> {} || {} -> {} == {}".format(
						# 	typeName, fieldName, relatedType, oldRelationalValues, mappedIds, { k : newRecord[k] for k in newRecord.fields_get_keys() })
						# )


						mappedIds = self._adjustRelationFieldCardinality(typeName, fieldName, mappedIds)
						if mappedIds != None:
							#self._info("{}.{} -> {} || {} -> {}".format(
							#	typeName, fieldName, relatedType, oldRelationalValues, mappedIds
							#))
							if type(mappedIds) == type([]):
								recordUpdate.setdefault(fieldName, []).append((6, 0, mappedIds))
							else:
								recordUpdate.setdefault(fieldName, mappedIds)
						#newRecord[fieldName] = mappedObjects


			for _id, recordUpdate in updateDict.items():
				#self._info("{}.{} -> {}".format(typeName, _id, recordUpdate))
				recordDict[_id].write(recordUpdate)


	def _mapIds(self, typeName, oldIds, generationNumber = None):

		if self._truncateType(typeName): return []

		if generationNumber != None: # we're in stage 0
			self._importType(typeName, generationNumber) # try to recurse

			if typeName in self.idMaps:
				mappingResult = []
				for oldId in oldIds:
					newId = self.idMaps[typeName].get(oldId)
					
					if newId != None:	mappingResult.append(newId)
					else: 			
						self._debug("unmapped id {}:{}".format(typeName, oldId))
						return None
			
				return mappingResult

			return None

		else: # stage 1 - all id's must be present
			idMap = self.idMaps[typeName]
			return list(map(lambda key: idMap[key], oldIds))


	def _resolveLeafs(self):

		if 'nameRef' in self.inputData:
			for leafType, perType in self.inputData['nameRef'].items():

				if self._truncateType(leafType): continue

				idMapForType = {}

				if not perType:
					#self._warn("Empty set of name references for type {} in input".format(leafType))
					continue
				
				typeInfo = self.getTypeInfo(leafType)
				
				self._info("resolving {} {} instances by name".format(len(perType), leafType))
				
				typeIsOptional = leafType in self.inputData['full']

				for dbId, names in perType.items():
					displayName, name, externalId = names

					# try name_search with the .display_name attribute of the original record
					matching = self.odooEnv[leafType].browse(
						[ idName[0] for idName in self.odooEnv[leafType].name_search(displayName, operator = '=') ]
					)
					self._debug("    LEAF: {} -> {} -> {} (displayName)".format(dbId, displayName, matching))

					# if not exact hit, try name_search with the .name attribute of the original record
					if len(matching) != 1 and name:
						matching = self.odooEnv[leafType].browse(
							[ idName[0] for idName in self.odooEnv[leafType].name_search(name, operator = '=') ]
						)
						self._debug("    LEAF: {} -> {} -> {} (ns name)".format(dbId, name, matching))

					# if not exact hit, try db search with name = .name of the original record
					if len(matching) != 1 and name:
						matching = self.odooEnv[leafType].search([['name', '=', name]])
						self._debug("    LEAF: {} -> {} -> {} (name)".format(dbId, name, matching))

					# try to "manually" filter all objects for an exact hit
					if len(matching) != 1:
						allInstances = self.odooEnv[leafType].search([['active', 'in', [True,False]]] if 'active' in typeInfo else [])
						self._debug("    LEAF: {} -> trying getAll ({} instances))".format(dbId, len(allInstances)))

						if externalId:
							byExternalId = allInstances.filtered(lambda r: r.get_external_id()[r.id] == externalId)
							self._debug("    LEAF: {} -> {} -> {} (name)".format(dbId, externalId, byExternalId))
						else:
							byExternalId = []


						if len(byExternalId) == 1:
							matching = set(byExternalId)
						else:
							matching = set().union(
								allInstances.filtered(lambda r: r.display_name == displayName) if displayName else []
							).union(
								allInstances.filtered(lambda r: r.name == name) if name and 'name' in typeInfo else []
							)

					if len(matching) == 1:
						idMapForType[dbId] = set(matching).pop().id # lazily force set type to use pop()

					elif not typeIsOptional:
						raise Exception("{} matches for instance of type '{}' with names '{}'/'{}'/'{}' : \n{}".format(len(matching), leafType, displayName, name, externalId, matching))

				self.idMaps.setdefault(leafType, {}).update(idMapForType)
				self._info("resolved {} {} instances by name: {}".format(len(idMapForType), leafType, self.idMaps[leafType]))

		# hardcoded (hopefully) static id of OdooBot, since it is not readable/exportable 
		self.idMaps.setdefault('res.partner', {})[2] = 2
					
					
