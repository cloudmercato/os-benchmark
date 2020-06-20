import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath('.'))))

project = 'Object Storage Benchmark'
copyright = '2020, Cloud Mercato'
author = 'Anthony Monthe'

extensions = [
    'sphinx.ext.autodoc',
]

templates_path = ['_templates']

exclude_patterns = []

html_theme = 'alabaster'
html_static_path = ['_static']
