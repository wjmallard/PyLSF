from distutils.core import setup, Extension

PyLSF = Extension(
    'PyLSF',
    sources = [
        'PyLSF.c'
    ],
    include_dirs = [
        '/lsf/7.0/include',
    ],
    libraries = [
        'lsf',
        'bat',
        'nss_nis',
        'm',
    ],
    library_dirs = [
        '/lsf/7.0/linux2.6-glibc2.3-x86_64/lib',
    ],
)

setup(
    name = 'PyLSF',
    version = '0.1',
    description = 'An LSF extension for Python.',
    author = 'William Mallard',
    author_email = 'wmallard@fas.harvard.edu',
    ext_modules = [PyLSF],
)

