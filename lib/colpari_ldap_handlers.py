# -*- coding: utf-8 -*-

from odoo.exceptions import ValidationError, UserError

#import odoo.addons.colpari_services.lib.colpari_ldap
#from . import colpari_ldap
import logging

import ldap
from ldap.filter import filter_format as lfff
import re


_logger = logging.getLogger(__name__)

class LDAPHandlerBase(object):

	def _makeIdentifier(self, name):
		name = name.replace(' '		, '_')
		name = re.sub(		r'[\W]+', '_', name)
		name = re.sub(		r'_+'	, '_', name)
		name = name.lstrip('_').rstrip('_')
		return name

	def checkObjectsForEntry(self, *odooObjects):
		''' checks if the right set of objects is provided for this handler and returns them grouped by type '''
		expectedTypes = self.requiredTypes().copy()
		groupedByType = {}
		for o in odooObjects:
			try: expectedTypes.remove(o._name)
			except: raise ValidationError("Did not expect object of type {} for LDAP association {}".format(o._name, self))
			groupedByType.setdefault(o._name, []).append(o)

		if len(expectedTypes) > 0:
			raise ValidationError("Missing additional object(s) for LDAP association of type {} : {}".format(self, expectedTypes))

		return groupedByType

	def _encodeSequence(self, seq):
		return [
			v.encode() if type(v) == type("") else v
				for v in seq
		]

	def getRDN(self, *odooObjects):
		return self.getRDNAttribute() + '=' + self.getRDNAttributeValue(*odooObjects)

	def getDN(self, serverConfig, *odooObjects):
		return self.getRDN(*odooObjects) + ',' + self.getBaseDN(serverConfig, *odooObjects)

	def createRecord(self, *odooObjects):
		return [ 
			(t[1], t[2]) for t in self.updateRecord(*odooObjects) # drop element 1, the mod operation
		] + [
			('objectclass'			, list(map(str.encode, self.getObjectClasses()))),
			(self.getRDNAttribute()	, self.getRDNAttributeValue(*odooObjects).encode())
		]

	def updateRecord(self, *odooObjects):
		values = self.getUpdateDict(*odooObjects)
		return [
			( ldap.MOD_REPLACE, k,  self._encodeSequence(v) )
				for k,v in values.items()
		]


	# to implement by derived classes
	# TODO: complete once the algo is finished
	def getRDNAttribute(self)							: raise NotImplementedError()
	def getRDNAttributeValue(self, *odooObjects)		: raise NotImplementedError()
	def getBaseDN(self, server, *odooObjects)			: raise NotImplementedError()
	def canSyncToLDAP(self, *odooObjects)				: raise NotImplementedError()
	def getUpdateDict(self, *odooObjects)				: raise NotImplementedError()
	def onDNChanged(self, oldDN, newDN, *odooObjects)	: raise NotImplementedError() #FIXME: get rid of this mechanism





class LDAPHandlerBase_groupOfNames(LDAPHandlerBase):

	def getRDNAttribute(self):
		return 'cn'

	def getRDNAttributeValue(self, *odooObjects):
		return self._makeCN(*odooObjects)

	def getObjectClasses(self):
		return ('groupOfNames',)

	def getBaseDN(self, serverConfig, *odooObjects):
		return serverConfig.base_dn_groups

	def getUpdateDict(self, *odooObjects):
		return {
			'description' 	: [self._makeDescription(*odooObjects) or ""],
			'member'		: ["cn=empty-membership-placeholder"]
		}

	def onDNChanged(self, oldDN, newDN, *odooObjects):
		''' called when the generated DN diverged from the actual DN. can return an update-record '''
		# update description
		return [( ldap.MOD_REPLACE, 'description', (self._makeDescription(*odooObjects) or "").encode() )]

	def _makeCN(self, *odooObjects):
		return self._makeIdentifier(self._makeDescription(*odooObjects))

	# abstract
	def canSyncToLDAP(self, *odooObjects)		: raise NotImplementedError()
	def _makeDescription(self, *odooObjects)	: raise NotImplementedError()



class LDAPHandler_serviceMembers(LDAPHandlerBase_groupOfNames):

	def requiredTypes(self):
		return ['project.project']

	def canSyncToLDAP(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		# yes, if enabled and name is not empty
		return odooObjects[0].create_authz_groups and odooObjects[0].display_name

	def _makeDescription(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		return "Service " + odooObjects[0].display_name



class LDAPHandler_serviceRoleMembers(LDAPHandlerBase_groupOfNames):

	def requiredTypes(self):
		return ['project.project', 'colpari.partner_role']

	def canSyncToLDAP(self, *odooObjects):
		(service, role) = self._getServiceAndRole(*odooObjects)
		# yes, if enabled and names are not empty
		return (service.create_authz_groups 
			and role.create_service_authz_groups
			and service.display_name
			and role.display_name )


	def _makeDescription(self, *odooObjects):
		(service, role) = self._getServiceAndRole(*odooObjects)
		return "Service " + service.display_name + " Role " + role.display_name

	def _getServiceAndRole(self, *odooObjects):
		grouped = self.checkObjectsForEntry(*odooObjects)
		return (grouped['project.project'][0], grouped['colpari.partner_role'][0])



class LDAPHandler_roleMembers(LDAPHandlerBase_groupOfNames):

	def requiredTypes(self):
		return ['colpari.partner_role']

	def canSyncToLDAP(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		# yes, if enabled and names are not empty
		return odooObjects[0].create_global_authz_groups and odooObjects[0].display_name

	def _makeDescription(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		return "Role " + odooObjects[0].display_name



class LDAPHandler_partner(LDAPHandlerBase):

	def requiredTypes(self):
		return ['res.partner']

	def getRDNAttribute(self):
		return 'uid'

	def getRDNAttributeValue(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		return self._makeUID(odooObjects[0])

	def getObjectClasses(self):
		return ('inetOrgPerson','organizationalPerson')

	def getBaseDN(self, serverConfig, *odooObjects):
		return serverConfig.base_dn_partners

	def canSyncToLDAP(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		p = odooObjects[0]
		return p.colpari_login_name and p.is_colpari_partner and p.colpari_partner_uid and not p.is_company

	def getUpdateDict(self, *odooObjects):
		self.checkObjectsForEntry(*odooObjects)
		p = odooObjects[0]
		givenName = " ".join(filter(None, (p.first_name, p.middle_name)))
		partnerEMail = p.colpari_login_name
		if not '@' in partnerEMail:
			partnerEMail += '@colpari.partners' #FIXME: make configurable
		return {
			'cn'						: [p.name or ""],
			'sn' 						: [p.last_name or ""],
			'givenName'					: [givenName or ""],
			'telephoneNumber' 			: [p.phone_line_number or "0"],
			'displayName' 				: [p.display_name or ""],
			'mail' 						: [p.email or ""],
			'employeeType' 				: ["active"],
			'physicalDeliveryOfficeName': [partnerEMail],
			'employeeNumber'			: [str(p.colpari_partner_uid) or "0000"]
		}

	def onDNChanged(self, oldDN, newDN, *odooObjects):
		''' called when the generated DN diverged from the actual DN. can return an update-record '''
		self.checkObjectsForEntry(*odooObjects)
		# update description
		return [( ldap.MOD_REPLACE, 'cn', odooObjects[0].display_name.encode() )]

	def _makeUID(self, partnerObj):
		# FIXME: do sanity check for value
		return partnerObj.colpari_login_name

__all__ = ('LDAPHandlerBase', 'LDAPHandler_serviceMembers', 'LDAPHandler_partner', 'LDAPHandler_serviceRoleMembers', 'LDAPHandler_roleMembers')
