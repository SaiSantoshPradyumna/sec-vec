import psutil
import subprocess
import time

def kill_process_using_file(filepath):
    for proc in psutil.process_iter(['pid', 'name', 'open_files']):
        try:
            for file in proc.info['open_files'] or []:
                if file.path == filepath:
                    proc.kill()
                    time.sleep(1)  # Wait for process to be terminated
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False

def delete_folder(folder_path):
    subprocess.run(['rd', '/s', '/q', folder_path], shell=True)
command = r'C:\kafka\bin\windows\kafka-storage.bat format -t FHNx76HFRlKLH1kLfJUO1g -c C:\kafka\config\kraft\server.properties'

folder_path = r'C:\kafka\kraft-combined-logs'
kill_process_using_file(folder_path)
delete_folder(folder_path)
subprocess.run(command, shell=True)

# EVENTS = <script>alert("XSS")</script>
# EVENTS=Speeding"); DROP TABLE car_event_logs; --,Engine On,Low tire pressure
# SELECT * FROM car_event_logs ORDER BY timestamp DESC;
