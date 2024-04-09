import cv2
import os
import time

import dask
import dask.threaded
from dask import delayed


# Функція для створення папки
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


# Функція для отримання кількості кадрів і FPS з відео
def get_video_info(video_path):
    cam = cv2.VideoCapture(video_path)
    total_frames = int(cam.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = int(cam.get(cv2.CAP_PROP_FPS))
    cam.release()
    return total_frames, fps


''' Sequential '''
# Функція для обробки одного відео
def extract_frames(video_path, output_folder_path):
    total_frames, fps = get_video_info(video_path)
    create_folder_if_not_exists(output_folder_path)

    cam = cv2.VideoCapture(video_path)
    current_frame = 0
    while True:
        ret, frame = cam.read()
        if ret:
            if current_frame % fps == 0:
                frame_path = os.path.join(output_folder_path, f"frame{current_frame//fps}.jpg")
                cv2.imwrite(frame_path, frame)
            current_frame += 1
        else:
            break

    cam.release()
    cv2.destroyAllWindows()

    print(f"sequential frames extracted and saved to {output_folder_path}")


''' Dask '''
# Функція для обробки одного відео (відкладена)
@delayed
def extract_frames_delayed_dask(video_path, output_folder_path):
    total_frames, fps = get_video_info(video_path)
    create_folder_if_not_exists(output_folder_path)

    cam = cv2.VideoCapture(video_path)
    current_frame = 0
    while True:
        ret, frame = cam.read()
        if ret:
            if current_frame % fps == 0:
                frame_path = os.path.join(output_folder_path, f"frame{current_frame//fps}.jpg")
                cv2.imwrite(frame_path, frame)
            current_frame += 1
        else:
            break

    cam.release()
    cv2.destroyAllWindows()

    print(f"Dask frames extracted and saved to {output_folder_path}")


''' Sequential '''
# Функція для обробки всіх відео в папці
def process_video_folder_sequentially(videos_folder_path, output_images_folder_path):
    try:
        # Цикл для обробки кожного відео у папці
        for video_file_name in os.listdir(videos_folder_path):
            video_path = os.path.join(videos_folder_path, video_file_name)
            output_folder_name = os.path.splitext(video_file_name)[0]  # Ім'я папки буде таке ж, як ім'я відео без розширення
            output_folder_path = os.path.join(output_images_folder_path, output_folder_name)

            extract_frames(video_path, output_folder_path)
    except Exception as e:
        print(f"Error processing video: {e}")


''' Dask '''
# Функція для обробки всіх відео в папці (відкладена)
def process_video_folder_parallel_dask(videos_folder_path, output_images_folder_path):
    try:
        delayed_tasks = []

        # Створення відкладених задач для кожного відео
        for video_file_name in os.listdir(videos_folder_path):
            video_path = os.path.join(videos_folder_path, video_file_name)
            output_folder_name = os.path.splitext(video_file_name)[0]
            output_folder_path = os.path.join(output_images_folder_path, output_folder_name)

            delayed_task = extract_frames_delayed_dask(video_path, output_folder_path)
            delayed_tasks.append(delayed_task)

        # Виконання відкладених задач паралельно з використанням Dask
        with dask.config.set(scheduler='threads', num_workers=4):  # Змініть кількість потоків за необхідності
            dask.compute(*delayed_tasks, scheduler=dask.threaded.get)

    except Exception as e:
        print(f"Error processing video folder: {e}")


# Шлях до папки з відео
videos_folder_path = "data/video/"


''' Sequential '''
# Шлях до папки для зображень
output_images_folder_path_sequential = "data/image/sequential/"

# Засікаємо початок виконання програми
start_time_sequential = time.time()

process_video_folder_sequentially(videos_folder_path, output_images_folder_path_sequential)

# Засікаємо кінець виконання програми
end_time_sequential = time.time()
execution_time_sequential = end_time_sequential - start_time_sequential
print(f"Total sequential execution time: {execution_time_sequential} seconds")


''' Dask '''
# Шлях до папки для зображень
output_images_folder_path_parallel_dask = "data/image/parallel/dask"

# Засікаємо початок виконання програми
start_time_parallel_dask = time.time()

process_video_folder_parallel_dask(videos_folder_path, output_images_folder_path_parallel_dask)

# Засікаємо кінець виконання програми
end_time_parallel_dask = time.time()
execution_time_parallel_dask = end_time_parallel_dask - start_time_parallel_dask
print(f"Total parallel dask execution time: {execution_time_parallel_dask} seconds")