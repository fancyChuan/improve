# -*- encoding: utf-8 -*-
"""
Created on 10:52 AM 10/4/2017

@author: fancyChuan
"""

import click

@click.command()
@click.option('--count', default=2, prompt='times', help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(int(count)):
        print count
        click.echo('Hello %s!' % name)

@click.command()
@click.option('--count', default=1, help='number of greetings')
@click.argument('name')
def hello2(count, name):
    for x in range(count):
        click.echo('Hello %s!' % name)
if __name__ == '__main__':
    hello2()