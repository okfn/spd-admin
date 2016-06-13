# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import click
from goodtables import pipeline
from data_quality import tasks, helpers, generators

@click.group()
def cli():
    """The entry point into the CLI."""

@cli.command()
@click.argument('config_filepath')
@click.option('--encoding', default=None)
@click.option('--deploy', is_flag=True)
def run(config_filepath, deploy, encoding):
    """Process data sources for a Spend Publishing Dashboard instance."""

    config = helpers.load_json_config(config_filepath)
    config['data_dir'] = helpers.resolve_relative_path(config_filepath, config['data_dir'])
    config['cache_dir'] = helpers.resolve_relative_path(config_filepath, config['cache_dir'])

    helpers.set_up_cache_dir(config['cache_dir'])
    source_filepath = os.path.join(config['data_dir'], config['source_file'])
    default_batch_options = {
        'goodtables': {
            'arguments': {
                'pipeline': {
                    'processors': ['structure', 'schema'],
                    'options': {
                        'schema': {'case_insensitive_headers': True}
                    }
                },
                'batch': {
                    'data_key': 'data'
                }
            }
        }
    }
    options = helpers.deep_update_dict(default_batch_options, config)
    aggregator = tasks.Aggregator(options)

    if deploy:

        def batch_handler(instance):
            aggregator.write_run()
            assesser = tasks.AssessPerformance(config)
            assesser.run()
            deployer = tasks.Deploy(config)
            deployer.run()

    else:

        def batch_handler(instance):
            aggregator.write_run()
            assesser = tasks.AssessPerformance(config)
            assesser.run()

    post_tasks = {'post_task': batch_handler, 'pipeline_post_task': aggregator.run}
    options['goodtables']['arguments']['batch'].update(post_tasks)
    batch_options = options['goodtables']['arguments']['batch']
    batch_options['pipeline_options'] = options['goodtables']['arguments']['pipeline']
    batch = pipeline.Batch(source_filepath, **batch_options)
    batch.run()


@cli.command()
@click.argument('config_filepath')
def deploy(config_filepath):
    """Deploy data sources for a Spend Publishing Dashboard instance."""

    config = helpers.load_json_config(config_filepath)
    config['data_dir'] = helpers.resolve_relative_path(config_filepath, config['data_dir'])
    deployer = tasks.Deploy(config)
    deployer.run()


@cli.command()
@click.argument('generator_class')
@click.argument('endpoint')
@click.argument('config_filepath', type=click.Path(exists=True))
@click.option('-gp','--generator_path', type=click.Path(exists=True), default=None,
              help='Full path to your custom generator (mandatory if you use a custom generator')
@click.option('-ft', '--file_type', multiple=True, default=['csv','excel'],
              help='File types that should be included in sources (default: csv and excel)')
def generate(generator_class, endpoint, config_filepath, generator_path, file_type):
    """Generate a database from the given endpoint

    Args:

        generator_class: Name of the generator class (ex: CkanGenerator)
        endpoint: Url where the generator should get the data from
        config_filepath: Path to the json config for data-quality-cli
    """
    if generator_class not in generators._built_in_generators:
        if generator_path is None:
            raise click.BadParameter(('You need to provide the path for your custom'
                                     'generator using the `--generator_path` option.'))

    file_types = list(file_type)
    config = helpers.load_json_config(config_filepath)
    config['data_dir'] = helpers.resolve_relative_path(config_filepath, config['data_dir'])
    generator = tasks.Generate(config)
    generator.run(generator_class, endpoint, generator_path, file_types)

if __name__ == '__main__':
    cli()
