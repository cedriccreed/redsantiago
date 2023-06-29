import argparse
import logging
import requests
import re

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ExtractDataDoFn(beam.DoFn):
    """Parse each dataset into the desired format."""
    def process(self, element):
        """Returns an iterator over the JSON elements.

        Args:
            element: the element being processed

        Returns:
            The processed element.
        """
        yield json.dumps(element)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', dest='output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # Obtener los datos de la API en formato JSON
        response = requests.get("https://datos.gob.cl/api/action/package_show?id=33245")
        data = response.json()

        # Obtener el resultado del dataset
        dataset = data['result']

        # Convertir los datos en el formato deseado
        formatted_data = p | 'FormatData' >> beam.Create([dataset]) | beam.ParDo(ExtractDataDoFn())

        # Guardar los datos en un archivo
        formatted_data | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
