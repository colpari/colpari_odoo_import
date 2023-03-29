# -*- coding: utf-8 -*-

from odoo.exceptions import ValidationError, UserError

#from . import colpari_ldap_handlers
#import odoo.addons.colpari_services.lib.colpari_ldap_handlers
from odoo.addons.colpari_services.lib.colpari_ldap_handlers import *
import logging

import ldap
from ldap.filter import filter_format as lfff
import re

_logger = logging.getLogger(__name__)

class ColpariLDAP(object):

	# member of all groupOfNames objects to not let them get technically empty
	groupOfNames_EmptyPlaceHolderMember = 'cn=empty-membership-placeholder'

	def __init__(self, server):
		c = server._connect(server)
		ldap_password = server['ldap_password'] or ''
		ldap_binddn = server['ldap_binddn'] or ''
		c.simple_bind_s(ldap_binddn, ldap_password)
		_logger.info("ColpariLDAP : aquired {} ".format(c))
		self.conn = c
		self.server = server
		self.env = server.env
		self.mappings = server.env['colpari.ldap_mapping']
		self.handlers = {
			'serviceMembers'	: LDAPHandler_serviceMembers(),
			'partner'			: LDAPHandler_partner(),
			'serviceRoleMembers': LDAPHandler_serviceRoleMembers(),
			'roleMembers'		: LDAPHandler_roleMembers()
		}

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if self.conn:
			_logger.info("ColpariLDAP : releasing {} ".format(self.conn))
			try:
				self.conn.unbind()
				self.conn = None
			except Exception as e:
				self.conn = None
				_logger.error('Exception while releasing LDAP connection: %s', e)


	def getUUIDforDN(self, dn, assertExists = False):
		try:
			result = self.conn.search_s(
				dn,
				ldap.SCOPE_BASE,
				attrlist=['entryUUID']
			)
		except ldap.NO_SUCH_OBJECT:
			result = []

		if len(result) == 0:
			if assertExists:
				raise ValidationError("Unable to find UUID for DN {}".format(dn))
			else:
				return None

		if len(result) == 1:
			uuids = result[0][1]['entryUUID']
			if len(uuids) > 1:
				raise ValidationError("DN {} has multiple uuids: {}".format(dn, uuids))

			return uuids[0].decode()

		raise ValidationError("Multiple search results for DN '{}'".format(dn))

	def getDNforUUID(self, uuid, assertExists = True):
		result = self.conn.search_s(
			self.server.ldap_base,
			ldap.SCOPE_SUBTREE,
			filterstr=lfff('(entryUUID=%s)', [uuid]),
			attrlist=['entryUUID']
		)
		if len(result) == 0:
			if assertExists:
				raise ValidationError("Unable to find DN for UUID {}".format(uuid))
			else:
				return None

		if len(result) == 1:
			return result[0][0]

		raise ValidationError("Multiple search results for uuid '{}'".format(uuid))

	def get(self, dn, *attrList):
		try:
			result = self.conn.search_s(
				dn,
				ldap.SCOPE_BASE,
				attrlist=attrList
			)

			if result: result = result[0]

			return result

		except ldap.NO_SUCH_OBJECT:
			return None

	def findObjects(self, baseDN, _filter, *attrList):
		return self.conn.search_s(
			baseDN,
			ldap.SCOPE_SUBTREE,
			_filter,
			attrList
		)

	def findSingleton(self, baseDN, _filter, *attrList):
		result = self.findObjects(baseDN, _filter, *attrList)
		resultLen = len(result)

		if resultLen == 0:
			return result

		if resultLen == 1:
			return result[0]

		raise ValidationError("Expected single result for {} below {} - got {}".format(_filter, baseDN, resultLen))

	def findSingletonByUUID(self, baseDN, uuid, *attrList):
		return self.findSingleton(baseDN, lfff('(entryUUID=%s)', [uuid]), *attrList)

	def findSingletonByUUIDAndClass(self, baseDN, uuid, objectClass, *attrList):
		return self.findSingleton(baseDN, lfff('(&(entryUUID=%s)(objectClass=%s))', [uuid, objectClass]), *attrList)

	def groupOfNamesMembers_getNamesbyUUID(self, baseDN, uuid):
		groupContent = self.findSingletonByUUIDAndClass(baseDN, uuid, 'groupOfNames')

		if len(groupContent) == 0:
			return None # not found

		if len(groupContent) == 2: # singleton ldap tuple (dn, dataDict)
			return list(map(bytes.decode, groupContent[1]['member']))

		raise ValidationError("Unexpected format for groupOfNames with uuid {} : {}".format(uuid, groupContent))

	def groupOfNamesMembers_getNames(self, dn):
		groupContent = self.get(dn, 'objectClass', 'member')

		#_logger.info("groupOfNamesMembers_getNames({}) =  {}".format(dn, groupContent))

		if not groupContent or (len(groupContent) == 0):
			return None # not found

		if len(groupContent) == 2: # singleton ldap tuple (dn, dataDict)
			classses = list(map(bytes.decode, groupContent[1]['objectClass']))
			if not 'groupOfNames' in classses:
				raise ValidationError("getNames requested for DN {} which does not have objectClass 'groupOfNames' but has classes {}".format(dn, classses))

			return list(map(bytes.decode, groupContent[1]['member']))

		raise ValidationError("Unexpected format for groupOfNames with dn {} : {}".format(dn, groupContent))

	def groupOfNamesMembers_syncMemberList(self, dn, expectedNames):

		expectedNames = set(expectedNames)
		expectedNames.add(ColpariLDAP.groupOfNames_EmptyPlaceHolderMember)

		currentNames = set(self.groupOfNamesMembers_getNames(dn) or [])

		# sync members
		toAdd 		= expectedNames - currentNames
		toRemove 	= currentNames - expectedNames

		modRecords = []

		for add in toAdd:
			modRecords.append((ldap.MOD_ADD, 'member', add.encode()))

		for remove in toRemove:
			modRecords.append((ldap.MOD_DELETE, 'member', remove.encode()))

		if modRecords:
			_logger.info("syncing groupOfNames: {} - adding {} / removing {} names".format(dn, len(toAdd), len(toRemove)))
			self.conn.modify_s(dn, modRecords)
		else:
			_logger.debug("syncing groupOfNames: {} nothing to do".format(dn))


	def syncServiceGroups(self, syncMode, colpariService, partnerId2DN = None):

		#_logger.info("{}.syncServiceGroups({}, {})".format(self, syncMode,colpariService))

		if not self.server.base_dn_groups:
			_logger.info("No base_dn_groups configured for server {} - not syncing {}".format(self.server, colpariService))
			return None

		if not colpariService.create_authz_groups:
			#FIXME: implement deleting groups if create_authz_groups becomes false
			return None

		# resolve all partners if not already provided
		if not partnerId2DN:
			partnerId2DN = self.syncAllPartners(syncMode)

		mapping = self._sync_generic('serviceMembers', syncMode, colpariService)

		if mapping:
			partnersInService = colpariService.partner_assignments.mapped('partner_id')
			dnsInService = set(filter(None, map(partnerId2DN.get, partnersInService.ids)))
			self.groupOfNamesMembers_syncMemberList(mapping.entry_ldap_dn, dnsInService)


			# sync serviceRoleMembers
			allRoles = self.env['colpari.partner_role'].search([])

			for r in allRoles:
				partnersInServiceRole = colpariService.partner_assignments.filtered(lambda a: r in a.role_ids).mapped('partner_id')
				# find mapped partner DNs which are in that role in this service
				roleDNs = set(filter(None, map(partnerId2DN.get, partnersInServiceRole.ids)))

				wasMappedBefore = self._findMapping('serviceRoleMembers', colpariService, r)

				if roleDNs or wasMappedBefore:
					# update if not empty or mapped before
					sRoleMapping = self._sync_generic('serviceRoleMembers', syncMode, colpariService, r)

					if not sRoleMapping:
						_logger.warning("Unable to sync 'serviceRoleMembers' for {} to LDAP".format((colpariService, r)))
					else:
						if r.create_service_authz_groups:
							self.groupOfNamesMembers_syncMemberList(sRoleMapping.entry_ldap_dn, roleDNs)
						else:
							#FIXME: delete LDAP object
							self.groupOfNamesMembers_syncMemberList(sRoleMapping.entry_ldap_dn, []) # empty the group for now



		return mapping

	def syncRoleGroups(self, syncMode, partnerId2DN = None):

		#_logger.info("{}.syncRoleGroups({})".format(self, syncMode))

		if not self.server.base_dn_groups:
			_logger.info("No base_dn_groups configured for server {} - not syncing role groups".format(self.server))
			return None

		allRoles = self.env['colpari.partner_role'].search([['create_global_authz_groups', '=', True]]) #FIXME: handle create_global_authz_groups below

		# resolve all partners if not already provided
		if not partnerId2DN:
			partnerId2DN = self.syncAllPartners(syncMode)

		result = []

		for role in allRoles:
			#FIXME: check if mapped and delete if create_global_authz_groups is false
			mapping = self._sync_generic('roleMembers', syncMode, role)
			if mapping:
				result.append(mapping)
				partnersInRole = role.project_assignments.mapped('partner_id')
				rolePartnerDNs = set(filter(None, map(partnerId2DN.get, partnersInRole.ids)))
				self.groupOfNamesMembers_syncMemberList(mapping.entry_ldap_dn, rolePartnerDNs)
			else:
				_logger.warning("Unable to sync 'roleMembers' for {} to LDAP".format(role))

		return result


	def syncPartner(self, syncMode, partner):
		mapping = self._sync_generic('partner', syncMode, partner)
		return mapping


	def syncAllPartners(self, syncMode):
		#_logger.info("{}.syncAllPartners({})".format(self, syncMode))
		partnerId2DN = {}
		for p in self.env['res.partner'].search([('is_colpari_partner', '=', True)]):
			mapping = self.syncPartner(syncMode, p)
			if mapping and mapping.entry_ldap_dn:
				partnerId2DN[p.id] = mapping.entry_ldap_dn
		return partnerId2DN


	def _findMapping(self, entryType, *odooObjects):
		return self.mappings.searchByServerTypeAndObjects(self.server, entryType, *odooObjects)

	def _createMapping(self, entryType, uuid, dn, *odooObjects):
		h = self.handlers[entryType]
		rdName = h.getRDNAttributeValue(*odooObjects)
		baseDN = h.getBaseDN(self.server, *odooObjects)
		newMapping = self.mappings.create([self.mappings.build(self.server, entryType, uuid, rdName, baseDN, dn, *odooObjects)])
		_logger.info("mapped {} {} = {}".format(entryType, odooObjects, newMapping.name))
		return newMapping

	def _create_generic(self, entryType, *odooObjects): #TODO: move to LDAPHandlerBase? (doesn't have ColpariLDAP access atm)

		if not entryType in self.handlers:
			raise UserError("EntryType {} not implemented".format(entryType))

		# get handler for entryType
		h = self.handlers[entryType]

		# gather data from handler
		dn 			= h.getDN(self.server, *odooObjects)
		addRecord 	= h.createRecord(*odooObjects)

		_logger.info("creating {} = {}".format(dn, addRecord))
		# write to ldap
		self.conn.add_s(dn, addRecord)

		# fetch uuid of new object
		newUUID = self.getUUIDforDN(dn, assertExists = True)

		return self._createMapping(entryType, newUUID, dn, *odooObjects)

	def _checkMappingAlive(self, mapping, syncMode):
		''' check if the state in the mapping matches with the server or if we can update it.
			see  _sync_generic() for syncMode
		'''
		uuid = self.getUUIDforDN(mapping.entry_ldap_dn)

		if uuid == mapping.entry_ldap_uuid:
			# the uuid for the cached DN maps back to the cached UUID - we can assume no major structural change has been done
			mapping.sync_state = 'sync'
			return mapping

		dn  = self.getDNforUUID(mapping.entry_ldap_uuid, assertExists = False) # check DN for cached UUID

		if dn and uuid:
			if (dn != mapping.entry_ldap_dn) and (uuid != mapping.entry_ldap_uuid):
				# cached DN & UUID are leading us in two different directions
				raise ValidationError(
					"Unable to match server state: cached UUID {} resolves to new DN {} and cached DN {} resolves to new UUID {}".format(
						mapping.entry_ldap_uuid, dn, mapping.entry_ldap_dn, uuid
					)
				)

		elif dn and (dn != mapping.entry_ldap_dn):
			if syncMode > 0:
				_logger.info("adopted new DN {} from cached UUID {} over cached DN {}".format(dn, mapping.entry_ldap_uuid, mapping.entry_ldap_dn))
				mapping.sync_state = "adopted new DN by cached UUID"
				mapping.entry_ldap_dn = dn
			else:
				mapping.sync_state = "seeing new DN by cached UUID"
				_logger.warning("seeing new DN {} for cached UUID {} over cached DN {}".format(dn, mapping.entry_ldap_uuid, mapping.entry_ldap_dn))

		elif uuid and (uuid != mapping.entry_ldap_uuid):
			if syncMode > 0:
				_logger.info("adopted new UUID {} from cached DN {} over cached UUID {}".format(uuid, mapping.entry_ldap_dn, mapping.entry_ldap_uuid))
				mapping.sync_state = "adopted new UUID by cached DN"
				mapping.entry_ldap_uuid = uuid
			else:
				mapping.sync_state = "adopted new UUID by cached DN"
				_logger.warning("seeing new UUID {} from cached DN {} over cached UUID {}".format(uuid, mapping.entry_ldap_dn, mapping.entry_ldap_uuid))

		elif not (dn or uuid): # it's dead, Jim
			if syncMode > 0:
				_logger.warning("Unable to resolve LDAP mapping {} - FORGETTING IT".format(mapping))
				mapping.unlink()
			else:
				_logger.warning("Unable to resolve LDAP mapping {}".format(mapping))
				mapping.sync_state = 'unable to resolve'

			mapping = None # do not elaborate on this in any casse


		return mapping


	def _sync_generic(self, entryType, syncMode, *odooObjects): #TODO: move to LDAPHandlerBase? (doesn't have ColpariLDAP access atm)
		'''	generic sync of a type+(set of odoo objects) in the specified mode
				syncMode: 0 = update at most the sync_state field existing mappings
				syncMode: 1 = update existing mappings or create new mappings referencing expected DN names found on the server
				syncMode: 2 = update existing LDAP objects or create new ones on the server
		'''
		# get handler for entryType
		h = self.handlers[entryType]

		mapping = self._findMapping(entryType, *odooObjects)

		if mapping and (len(mapping) > 1):
			raise ValidationError("Multiple LDAP mappings for {} {}".format(entryType, odooObjects))

		if not h.canSyncToLDAP(*odooObjects):
			#TODO: ideally this should happen after _checkMappingAlive() to FIXME: implement delete-mode
			_logger.info("not syncing {} object(s) {}".format(entryType, odooObjects))
			if mapping:
				#TODO: delete LDAP object and mapping in sync mode 3?
				mapping.sync_state = 'not syncable'
			return None

		_logger.debug("syncing {} object(s) {} in mode {}".format(entryType, odooObjects, syncMode))


		if mapping and mapping.sync_state:
			# was synced before - only check if objects where modified
			newestObjectTime = max(map(lambda x : x.write_date, odooObjects))
			if newestObjectTime < mapping.write_date:
				_logger.debug("not syncing {} object(s) {} in mode {} because mapping is newer than data".format(entryType, odooObjects, syncMode))
				return mapping

		if mapping:
			# ok, if we have one we check if it maps. might return None
			mapping = self._checkMappingAlive(mapping, syncMode)


		expectedRDN = h.getRDN(*odooObjects)
		expectedBDN = h.getBaseDN(self.server, *odooObjects)
		expectedDN 	= expectedRDN + ',' + expectedBDN

		updateExistingObject = True

		if mapping:
			# mapping is sync. check if DN is still == expectedDN - rename if not
			# FIXME: distinct between RDN changed and BDN changed - in the latter case, the modrdn ldap operation below will fail
			if expectedDN != mapping.entry_ldap_dn:
				if syncMode > 1:
					# generated DN is different from existing DN
					_logger.info("{} entry for {} with cached uuid {} changed DN ({} vs {})- updating".format(
						entryType, odooObjects, mapping.entry_ldap_uuid, mapping.entry_ldap_dn, expectedDN
					))
					self.conn.modrdn_s(mapping.entry_ldap_dn, expectedRDN)
					modRecord = h.onDNChanged(mapping.entry_ldap_dn, expectedDN, *odooObjects)

					if modRecord:
						self.conn.modify_s(expectedDN, modRecord)

					mapping.entry_ldap_dn = expectedDN
					mapping.sync_state = 'sync'
				else:
					mapping.sync_state = 'to be moved to new DN'


		else:
			# no mapping yet or we dropped it above
			if syncMode > 0:
				# see if we find data under the expectedDN
				existingData = self.get(expectedDN, 'objectClass', 'entryUUID')
				if existingData:
					attrs = existingData[1]
					existingClasses = set(map(bytes.decode, attrs['objectClass']))
					expectedClasses = set(h.getObjectClasses())
					if expectedClasses != existingClasses:
						raise ValidationError("DN {} exists but is unmapped and has unexpected objectClass(es): expected {}  actual: {}".format(
							expectedDN, expectedClasses, existingClasses
						))
					# DN exists and has expected classes - we take that
					dn = expectedDN
					uuid = set(map(bytes.decode, attrs['entryUUID'])).pop()
					mapping = self._createMapping(entryType, uuid, dn, *odooObjects)
					mapping.sync_state = "adopted by expected DN"


				elif syncMode > 1: # nothing found and we are allowed to create
					mapping = self._create_generic(entryType, *odooObjects)
					mapping.sync_state = "object created on server"
					updateExistingObject = False # because we just created it


		# mapping definitely exists now
		if (syncMode > 1) and updateExistingObject:
			_logger.info("updating data for {} ".format(mapping.name))
			self.conn.modify_s(mapping.entry_ldap_dn, h.updateRecord(*odooObjects))

		# FIXME: this is needed because "bdn + rdn = dn"-handling is not stringent enough
		if syncMode > 0:
			mapping.entry_rd_name = h.getRDNAttributeValue(*odooObjects)
			mapping.entry_ldap_base_dn = expectedBDN


		return mapping
