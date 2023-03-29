# -*- coding: utf-8 -*-

from . import colpari_odoo_import

def post_init(cr, registry):
	import logging

	_logger = logging.getLogger(__name__)
	_logger.info("post_init CALLED")
