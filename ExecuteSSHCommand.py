"""This module executes SSH Command."""
#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
from LogSetup import logger
import traceback
from select import select
from ssh_hook import SSHHook


class ExecuteSSHCommand(object):
    """This class executes SSH Command."""

    def __init__(self, remote_host, username, password=None, key_file=None, timeout=10):
        self.ssh_hook = SSHHook(remote_host=remote_host, username=username, password=password,
                                key_file=key_file)
        self.timeout = timeout

    def execute_command(self, command):
        """This method executes SSH command entered."""
        if not self.ssh_hook:
            raise Exception("SSH hook not initialized")

        if not command:
            raise Exception("No command specified")

        ssh_client = self.ssh_hook.get_conn()

        # Auto apply tty when its required in case of sudo
        get_pty = False
        if command.startswith("sudo"):
            get_pty = True

        # set timeout taken as params
        stdin, stdout, stderr = ssh_client.exec_command(command=command,
                                                        get_pty=get_pty,
                                                        timeout=self.timeout
                                                       )
        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        agg_stdout = b''
        agg_stderr = b''

        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        line = None

        # read from both stdout and stderr
        while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], self.timeout)
            for c in readq:
                if c.recv_ready():
                    line = stdout.channel.recv(len(c.in_buffer))
                    line = line
                    agg_stdout += line
                    logging.info(line.decode('utf-8').strip('\n'))
                if c.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                    line = line
                    agg_stderr += line
                    logging.warning(line.decode('utf-8').strip('\n'))
            if stdout.channel.exit_status_ready()\
                    and not stderr.channel.recv_stderr_ready()\
                    and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()

        exit_status = stdout.channel.recv_exit_status()
        if exit_status is 0:
            value = line
            logging.info("RETURNED----%s", str(value))
            return line.rstrip(b"\n\r")
        else:
            error_msg = agg_stderr.decode('utf-8')
            status_message = "error running cmd: {0}, error: {1}".format(command, error_msg)
            raise Exception("error running cmd: {0}, error: {1}".format(command, error_msg))


if __name__ == "__main__":
    OBJ = ExecuteSSHCommand(remote_host="10.228.32.79", username="hadoop",
                            key_file="/home/test.pem")
