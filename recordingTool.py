import ffmpeg
import time
from datetime import datetime, timedelta
import json
import logging
import sys
import subprocess
import os
import shutil


def check_json_config(config):
    rtsp_list = []
    camera_list = []
    cameras = config.get("cameras", [])
    for index, element in enumerate(cameras):
        if "rtsp_url" not in element or "camera_name" not in element:
            logging.error(f"Element at index {index} is missing 'rtsp_url' or 'camera_name'.")
            print(f"Error: Element at index {index} is missing 'rtsp_url' or 'camera_name'. Exiting.")
            sys.exit(1)
        elif element.get('rtsp_url') in rtsp_list or element.get('camera_name') in camera_list:
            logging.error(f"'rtsp_url' or 'camera_name' at index {index} is duplicated.")
            print(f"Error: 'rtsp_url' or 'camera_name' at index {index} is duplicated.")
            sys.exit(1)
        rtsp_list.append(element.get('rtsp_url'))
        camera_list.append(element.get('camera_name'))


def generate_filename(camera_name):
    current_date_str = datetime.now().strftime("%Y%m%d")
    folder_path = os.path.join(current_date_str)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(folder_path, f"{camera_name}_{timestamp}.ts")


def remove_old_folders(video_keep_day):
    cutoff_date = datetime.now() - timedelta(days=video_keep_day)
    cutoff_date_str = cutoff_date.strftime("%Y%m%d")

    for folder_name in os.listdir():
        if folder_name.isdigit() and len(folder_name) == 8:
            if folder_name < cutoff_date_str:
                folder_path = os.path.join(folder_name)
                if os.path.isdir(folder_path):
                    logging.info(f"Removing old folder: {folder_path}")
                    shutil.rmtree(folder_path)
                    print(f"Removed folder: {folder_path}")


def record_stream(config):
    last_record_date = None
    video_keep_day = config.get("video_keep_day", 7)

    while True:
        current_date = datetime.now().date()

        if last_record_date is None or current_date != last_record_date:
            last_record_date = current_date

            remove_old_folders(video_keep_day)

            processes = dict()
            cameras = config.get("cameras", [])
            while True:
                for info in cameras:
                    camera_name = info.get('camera_name')
                    rtsp_url = info.get('rtsp_url')

                    if camera_name not in processes.keys():
                        output_file = generate_filename(camera_name=camera_name)
                        logging.info(f"Starting recording to {output_file}")
                        print(f"Starting recording to {output_file}")

                        try:
                            process = (
                                ffmpeg
                                .input(rtsp_url, rtsp_transport='tcp', buffer_size='1000000')
                                .output(output_file, format='mpegts', vcodec='copy', acodec='copy', g=10)
                                .global_args('-loglevel', 'quiet')
                                .run_async(pipe_stdout=True, pipe_stderr=True)
                            )
                            processes[camera_name] = process
                        except subprocess.CalledProcessError as e:
                            logging.error(f"Recording interrupted due to a subprocess error: {e}")
                            logging.info("Retrying in 5 seconds...")
                            time.sleep(5)
                        except Exception as e:  # 捕獲所有其他異常
                            logging.error(f"Recording interrupted due to an unexpected error: {str(e)}")
                            logging.info("Retrying in 5 seconds...")
                            time.sleep(5)
                        except KeyboardInterrupt:
                            logging.info("Recording stopped by user.")
                            for process in processes.values():
                                try:
                                    process.terminate()
                                    process.wait()
                                except Exception as e:
                                    logging.error(f"Error terminating process during exit: {str(e)}")
                            sys.exit(1)

                        time.sleep(3)
                    else:
                        process = processes.get(camera_name, None)
                        if not process:
                            print("FFmpeg process terminated unexpectedly.")
                            logging.error("FFmpeg process terminated unexpectedly. Restarting...")
                            processes.pop(camera_name)
                        if process.poll() is not None:
                            print("FFmpeg process terminated unexpectedly.")
                            logging.error("FFmpeg process terminated unexpectedly. Restarting...")
                            processes.pop(camera_name, None)
                            try:
                                process.terminate()
                                process.wait()
                            except Exception as e:
                                logging.error(f"Error terminating process for {camera_name}: {str(e)}")
                            time.sleep(60)

                            output_file = generate_filename(camera_name=camera_name)
                            try:
                                process = (
                                    ffmpeg
                                    .input(rtsp_url)
                                    .output(output_file, format='mpegts', vcodec='copy', acodec='copy')
                                    .global_args('-loglevel', 'quiet')
                                    .run_async(pipe_stdout=True, pipe_stderr=True)
                                )
                                processes[camera_name] = process
                            except Exception as e:
                                logging.error(f"Failed to restart recording for {camera_name}: {str(e)}")

                if datetime.now().date() != last_record_date:
                    print("It's a new day! Restarting recording...")
                    for camera_name, process in processes.items():
                        try:
                            process.terminate()
                            process.wait()
                        except Exception as e:
                            logging.error(f"Error terminating process for {camera_name}: {str(e)}")
                    break

                time.sleep(5)


if __name__ == "__main__":
    try:
        with open("params.json", "r") as json_file:
            config = json.load(json_file)
    except json.JSONDecodeError as e:
        logging.error(f"Error reading JSON file: {str(e)}")
        print("Error: Invalid JSON format. Exiting.")
        sys.exit(1)

    check_json_config(config=config)
    logging.basicConfig(filename='recording.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s: %(message)s')
    print(config)
    try:
        record_stream(config=config)
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {str(e)}")
        input("An error occurred in main. Press Enter to exit...")
