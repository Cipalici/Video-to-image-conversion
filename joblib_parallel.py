import cv2
import os
import time

from joblib import Parallel, delayed


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


''' Joblib '''
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

    print(f"Joblib frames extracted and saved to {output_folder_path}")


''' Joblib '''
# Функція для обробки всіх відео в папці
def process_video_folder_parallel_joblib(videos_folder_path, output_images_folder_path):
    try:
        # Створення відкладених задач для кожного відео
        delayed_tasks = []
        for video_file_name in os.listdir(videos_folder_path):
            video_path = os.path.join(videos_folder_path, video_file_name)
            output_folder_name = os.path.splitext(video_file_name)[0]
            output_folder_path = os.path.join(output_images_folder_path, output_folder_name)

            delayed_task = delayed(extract_frames)(video_path, output_folder_path)
            delayed_tasks.append(delayed_task)

        # Виконання відкладених задач паралельно з використанням Joblib і бекендом threading
        Parallel(n_jobs=4, backend="threading")(delayed_tasks)

    except Exception as e:
        print(f"Error processing video folder: {e}")


# Шлях до папки з відео
videos_folder_path = "data/video/"


''' Joblib '''
# Шлях до папки для зображень
output_images_folder_path_parallel_joblib = "data/image/parallel/joblib"

# Засікаємо початок виконання програми
start_time_parallel_joblib = time.time()

process_video_folder_parallel_joblib(videos_folder_path, output_images_folder_path_parallel_joblib)

# Засікаємо кінець виконання програми
end_time_parallel_joblib = time.time()
execution_time_parallel_joblib = end_time_parallel_joblib - start_time_parallel_joblib
print(f"Total parallel joblib execution time: {execution_time_parallel_joblib} seconds")
