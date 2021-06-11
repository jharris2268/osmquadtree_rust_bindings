from setuptools import setup, find_namespace_packages
from setuptools_rust import Binding, RustExtension


setup(
    name='osmquadtree_rust_bindings',
    version="0.1.0",
    packages=['osmquadtree_rust_bindings'],
    zip_safe=False,
    rust_extensions=[RustExtension("osmquadtree_rust_bindings.rust", path="Cargo.toml", binding=Binding.PyO3, debug=False)],
)
