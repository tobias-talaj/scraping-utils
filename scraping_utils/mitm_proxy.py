"""
Module to control and interact with MitmProxy to capture HTTP requests and responses.

This module provides classes to start and stop a MitmProxy instance programmatically,
capture HTTP requests and responses, and search captured data for specified keywords.
"""
import re
import json
import time
import socket
import asyncio
import threading
from scraping_utils import setup_logging

from mitmproxy import http
from mitmproxy.options import Options
from mitmproxy.tools.dump import DumpMaster


class HeaderSaver:
    def __init__(self):
        self.requests = []
        self.responses = []

    def request(self, flow: http.HTTPFlow) -> None:
        request_data = {
            "method": flow.request.method,
            "url": flow.request.url,
            "headers": dict(flow.request.headers),
            "payload": self._get_payload(flow.request)
        }
        self.requests.append(request_data)

    def response(self, flow: http.HTTPFlow) -> None:
        response_data = {
            "status_code": flow.response.status_code,
            "reason": flow.response.reason,
            "headers": dict(flow.response.headers)
        }
        self.responses.append(response_data)

    def _get_payload(self, request: http.Request) -> dict | str:
        content_type = request.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                return json.loads(request.content.decode('utf-8'))
            except json.JSONDecodeError:
                return request.content.decode('utf-8')
        elif "application/x-www-form-urlencoded" in content_type:
            return dict(request.urlencoded_form)
        else:
            return request.content.decode('utf-8')

    def get_captured_data(self) -> dict:
        return {
            "requests": self.requests,
            "responses": self.responses
        }


class MitmProxyController:
    def __init__(self, name, host: str = '0.0.0.0', port: int = 8080):
        self.logger = setup_logging(f"mitmproxy_{name}.log")
        self.host = host
        self.port = port
        self.master = None
        self.mitmproxy_thread = None
        self.should_exit = threading.Event()
        self.header_saver = None

    async def run_master(self) -> None:
        options = Options(listen_host=self.host, listen_port=self.port)
        self.master = DumpMaster(options)
        self.header_saver = HeaderSaver()
        self.master.addons.add(self.header_saver)
        await self.master.run()

    def start(self) -> None:
        def run():
            asyncio.run(self.run_master())

        self.logger.info("Starting mitmproxy")

        self.mitmproxy_thread = threading.Thread(target=run)
        self.mitmproxy_thread.start()

        self._wait_until_proxy_is_ready()

    def _wait_until_proxy_is_ready(self) -> None:
        start_time = time.time()
        timeout = 10
        while True:
            try:
                with socket.create_connection((self.host, self.port), timeout=1):
                    self.logger.info("mitmproxy is ready")
                    return
            except OSError:
                time.sleep(0.5)
                if time.time() - start_time > timeout:
                    self.logger.error("Timeout: mitmproxy didn't start within 10 seconds")
                    raise Exception("Timeout: mitmproxy didn't start within 10 seconds")

    def stop(self) -> None:
        if self.master:
            self.logger.info("Stopping mitmproxy...")
            self.master.shutdown()
            if self.mitmproxy_thread:
                self.mitmproxy_thread.join()
            self.logger.info("mitmproxy stopped")

    def find_requests_with_keywords(self, keywords: list[str]) -> list:
        matching_requests = []
        if self.header_saver:
            for request_data in self.header_saver.requests:
                request_str = str(request_data)
                if any(self.word_in_string(keyword, request_str) for keyword in keywords):
                    matching_requests.append(request_data)
        return matching_requests
    
    def word_in_string(self, word: str, text: str) -> bool:
        pattern = fr'\b{re.escape(word)}\b'
        return bool(re.search(pattern, text, re.IGNORECASE))
