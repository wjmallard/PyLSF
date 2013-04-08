from distutils.core import setup, Extension

# To build the module:
# $ python setup.py build
# $ mv build/lib.XYZ/PyLSF.so .
# $ rm -rf build
#
# To use the module:
# >>> import PyLSF
#
# Make sure LD_LIBRARY_PATH includes the LSF lib dir.

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
    description = 'An LSF wrapper for the Lincer pipeline.',
    author = 'William Mallard',
    author_email = 'wmallard@fas.harvard.edu',
    ext_modules = [PyLSF],
)
