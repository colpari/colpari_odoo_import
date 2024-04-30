# -*- coding: utf-8 -*-
{
    'name': "colpari odoo import",

    'summary': "import data from other odoo instances",

    'description': "Flexible import of data from other odoo instances",

    'author': "colpari",
    'website': "https://colpari.cx",

    'category': 'Administration',
    'version': '0.7',

    'application': True,

    # any module necessary for this one to work correctly
    'depends': [
        'base'
    ],

    # always loaded
    'data': [
        'security/groups.xml',
        'views/addView_import_runs.xml',
        'views/addView_import_configs.xml',
        'views/addView_import_sources.xml',
        # 'views/addView_colpari.action_type.xml',
        # 'views/addView_colpari.action_unit.xml',
        # 'views/addView_colpari.activity_value_import_zammad.xml',
        # 'views/addView_colpari.activity_value_import_3cx.xml',
        # 'views/addView_colpari.activity_value_import_run.xml',
        # 'views/addView_colpari.feature.set.xml',
        # 'views/addView_colpari.feature.xml',
        # 'views/addView_colpari.partner_role.xml',
        # 'views/addView_colpari.project_partner_role.xml',
        # 'views/addView_colpari.action_price_list.xml',
        # 'views/addView_colpari.service_level.xml',
        # 'views/addView_colpari.settlement.xml',
        # 'views/addView_colpari.indicator.xml',
        # 'views/addView_colpari.ldap_mapping.xml',
        # 'views/addView_colpari.activity.xml',
        # 'views/addView_colpari.activity_payments.xml',
        # 'views/addView_project.project-tree.xml',
        # 'views/addView_res.partner-clients.xml',
        # 'views/addView_res.partner-partners.xml',
        # 'views/addView_res_config_settings.xml',
        # 'views/addViews_skills.xml',
        # 'views/changeForm_project.project.xml',
        # 'views/changeForm_orders.xml',
        # 'views/changeForm_res.partner.xml',
        # 'views/changeForm_res_company_ldap.xml',
        # 'views/changeKanban_project.project.xml',
        'views/main_menu.xml',

        # 'data/uom.uom.csv',
        # 'data/colpari.action_type.csv',
        # 'data/colpari.action_unit.csv',
        # 'data/colpari.indicator.csv',
        # 'data/colpari.feature.set.csv',
        # 'data/colpari.feature.csv',
        'security/ir.model.access.csv'
    ],

    # 'demo': [
    #     'demo/demo.xml',
    # ],

    #'qweb': [
    #    'static/src/xml/resume_templates.xml',
    #    'static/src/xml/skills_templates.xml',
    #],
    # only loaded in demonstration mode
}
