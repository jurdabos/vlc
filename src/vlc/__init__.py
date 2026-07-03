"""Top-level package for the vlc project CLI.

Hosts the command-line interface of the Valencia air quality and weather
data pipeline. Pipeline services (producer, consumer) remain standalone
directories consumed by their Docker images; this package only carries
host-side tooling.
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
