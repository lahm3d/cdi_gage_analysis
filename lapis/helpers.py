import subprocess

def run_command(command):
    """
    Run a command in the system shell and return the result.

    Args:
        command (str): The command to be executed in the system shell.

    Returns:
        CompletedProcess: An object containing the result of the command execution.

    Raises:
        Exception: If the command execution fails and returns a non-zero exit code.

    """
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if result.returncode != 0:
        raise Exception(f"Run failed. Return from stderr: {result.stderr}")

    return result