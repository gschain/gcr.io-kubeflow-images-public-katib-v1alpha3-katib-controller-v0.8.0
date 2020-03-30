#!/usr/bin/env python
# coding: utf-8

import tornado.httpserver
import tornado.httpclient
import tornado.ioloop
import tornado.autoreload
import tornado.web
import tornado.gen
import json
import nbformat
import os
import boto.s3.connection
import shutil
import subprocess
from boto.s3.key import Key
from boto.exception import S3ResponseError
from nbconvert.preprocessors import ExecutePreprocessor, CellExecutionError
from nbformat import NBFormatError
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
from distutils.util import strtobool
import configparser


def config():
    curpath = os.path.dirname(os.path.realpath(__file__))
    cfgpath = os.path.join(curpath, 'nbdeploy.ini')
    conf = configparser.ConfigParser()
    conf.read(cfgpath, encoding='utf-8')
    return conf


class S3Storage(object):
    __default_bucket = 'nbexecutor'
    __cfg = config()

    def __init__(self, bucket=__default_bucket):
        self._host = self.__cfg.get('s3', 'host')
        self._port = int(self.__cfg.get('s3', 'port'))
        self._access_key = self.__cfg.get('s3', 'access')
        self._secret_key = self.__cfg.get('s3', 'secret')
        self._conn = boto.connect_s3(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            host=self._host,
            port=self._port,
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )
        try:
            self.bucket = self._conn.get_bucket(bucket)
        except S3ResponseError:
            self.create_bucket(bucket)
        self.k = Key(self.bucket)

    def create_bucket(self, bucket=__default_bucket):
        self.bucket = self._conn.create_bucket(bucket)

    def download_file(self, uniq_id, filename=''):
        if filename == '':
            filename = 'jobConf.json'
        self.k.key = uniq_id + '/' + filename
        local_dir = self.__cfg.get('local', 'exedir') + '/' + uniq_id
        if os.path.exists(local_dir):
            self.k.get_contents_to_filename(local_dir + '/' + filename)
        else:
            try:
                os.mkdir(local_dir, 0o755)
                try:
                    data = self.k.get_contents_as_string()
                    with open(local_dir + '/' + filename, 'w') as f:
                        f.write(data.decode())
                except IOError as e:
                    print(e)
            except IOError as e:
                print(e)

    def download_s3(self, uniq_id, filename=''):
        try:
            self.download_file(uniq_id, filename)
        except S3ResponseError as e:
            print(e)

    def _parse_config_file(self, uniq_id):
        return self._get_running_config(uniq_id)

    def download_resources(self, uniq_id):
        self.download_s3(uniq_id)
        config = self._parse_config_file(uniq_id)
        self.download_s3(uniq_id, config['running_script'])
        if config['running_resources']:
            for running_resource in config['running_resources']:
                self.download_s3(uniq_id, running_resource)

    def _get_running_config(self, uniq_id):
        local_file = self.__cfg.get('local', 'exedir') + '/' + uniq_id + '/jobConf.json'
        try:
            with open(local_file, 'rb') as f:
                data = f.read().decode()
            return json.loads(data)
        except IOError as e:
            print(e)


class IndexHandler(tornado.web.RequestHandler):
    async def get(self):
        self.write('Notebook Execution Restful API version 1.0.0')


class VersionHandler(tornado.web.RequestHandler):
    async def get(self):
        version = {'version': 'v1', 'author': 'xianglei'}
        self.write(json.dumps(version, ensure_ascii=False, indent=4))


class ExecuteHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(max_workers=16)
    __cfg = config()

    @tornado.gen.coroutine
    def post(self, uniq_id):
        config = self._get_config(uniq_id)
        ret = yield self._run_notebook(config)
        self.write(json.dumps(ret, ensure_ascii=False, indent=4))
        # self.write(json.dumps(config, ensure_ascii=False, indent=4))
        # self._run_notebook(filename)

    def _get_config(self, uniq_id):
        s3 = S3Storage()
        s3.download_resources(uniq_id)
        return s3._get_running_config(uniq_id)

    @run_on_executor
    def _run_notebook(self, running_config):
        self.uniq_id = running_config['uniq_id']
        self.running_script = running_config['running_script']
        self.running_params = running_config['running_params']
        self.running_resources = running_config['running_resources']
        self.local_dir = self.__cfg.get('local', 'exedir') + '/' + self.uniq_id + '/'
        file_type = self.running_script.split('.')
        if file_type[-1] == 'ipynb':
            ret = self.__run_ipynb()
        elif file_type[-1] == 'sh':
            ret = self.__run_sh()
        else:
            ret = {'statusCode': 500, 'data': 'Unknown file type'}
        return ret

    def __run_sh(self):
        try:
            if self.running_resources:
                running_resources = ','.join(self.running_resources)
                running = ['bash', self.running_script, self.running_params, running_resources]
            else:
                running = ['bash', self.running_script, self.running_params]
            iostream = subprocess.Popen(running, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = iostream.communicate()
            if stdout:
                ret = {'statusCode': 200, 'data': stdout.decode()}
            else:
                ret = {'statusCode': 501, 'data': stderr.decode()}
            return ret

        except OSError as e:
            print(e)

    def __run_ipynb(self):
        try:
            nb = nbformat.read(self.local_dir + self.running_script, as_version=4)
        except NBFormatError as e:
            msg = str(e)
            nb = None
        ep = ExecutePreprocessor(kernel_name='python3')
        if nb != None:
            try:
                out = ep.preprocess(nb, {'metadata': {'path': self.__cfg.get('local', 'exedir') +
                                         '/' + self.uniq_id + '/'}})
            except CellExecutionError:
                out = (None,)
                msg = 'Error executing the notebook "%s".\n\n' % self.local_dir + self.running_script
            finally:
                shutil.rmtree(self.local_dir)
        else:
            raise OSError

        data = out[0]
        if out[0] != None:
            stdouts = ''
            for cell in data['cells']:
                for output in cell['outputs']:
                    stdouts += output['text']
            ret = {'statusCode': 200, 'data': stdouts}
        else:
            ret = {'statusCode': 500, 'data': 'Internal Server Error: ' + msg}
        return ret


class Application(tornado.web.Application):
    __cfg = config()

    def __init__(self):
        handlers = [
            (r'/', IndexHandler),
            (r'/nb/v1/version', VersionHandler),
            (r'/nb/v1/execute/(.*)', ExecuteHandler),
            # (r'/nb/v1/register/(.*)', RegisterHandler),
        ]
        settings = {
            'cookie_secret': 'HeavyMetalWillNeverDie!!!',
            'xsrf_cookies': False,
            'gzip': True,
            'debug': strtobool(self.__cfg.get('local', 'debug')),
            'autoreload': strtobool(self.__cfg.get('local', 'autoreload')),
            'xheaders': True
        }
        tornado.web.Application.__init__(self, handlers, **settings)


if '__main__' == __name__:
    __cfg = config()
    server = tornado.httpserver.HTTPServer(Application())
    server.listen(int(__cfg.get('local', 'port')))
    loop = tornado.ioloop.IOLoop.instance()
    tornado.autoreload.start(loop)
    loop.start()
