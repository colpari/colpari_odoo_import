# -*- coding: utf-8 -*-
# from odoo import http


# class ColpariData(http.Controller):
#     @http.route('/colpari_data/colpari_data/', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/colpari_data/colpari_data/objects/', auth='public')
#     def list(self, **kw):
#         return http.request.render('colpari_data.listing', {
#             'root': '/colpari_data/colpari_data',
#             'objects': http.request.env['colpari_data.colpari_data'].search([]),
#         })

#     @http.route('/colpari_data/colpari_data/objects/<model("colpari_data.colpari_data"):obj>/', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('colpari_data.object', {
#             'object': obj
#         })
