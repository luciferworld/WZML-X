from io import BufferedReader
from logging import getLogger
from os import path as ospath
from os import walk as oswalk
from pathlib import Path

from aiofiles.os import path as aiopath
from aiohttp import ClientSession, FormData
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bot.core.config_manager import Config
from bot.helper.ext_utils.bot_utils import SetInterval, sync_to_async
from bot.helper.ext_utils.telegraph_helper import telegraph

LOGGER = getLogger(__name__)


class ProgressFileReader(BufferedReader):
    def __init__(self, filename, read_callback=None):
        super().__init__(open(filename, "rb"))
        self.__read_callback = read_callback
        self.length = Path(filename).stat().st_size

    def read(self, size=None):
        size = size or (self.length - self.tell())
        if self.__read_callback:
            self.__read_callback(self.tell())
        return super().read(size)

    def __len__(self):
        return self.length


class VikingFileUpload:
    def __init__(self, listener, path):
        self.listener = listener
        self._updater = None
        self._path = path
        self._is_errored = False
        self.api_url = "https://vikingfile.com/api"
        self.__processed_bytes = 0
        self.last_uploaded = 0
        self.total_time = 0
        self.total_files = 0
        self.total_folders = 0
        self.is_uploading = True
        self.update_interval = 3

        from bot import user_data

        user_dict = user_data.get(self.listener.user_id, {})
        self.user_hash = user_dict.get("VIKINGFILE_USER") or Config.VIKINGFILE_USER

    @property
    def speed(self):
        try:
            return self.__processed_bytes / self.total_time
        except Exception:
            return 0

    @property
    def processed_bytes(self):
        return self.__processed_bytes

    def __progress_callback(self, current):
        chunk_size = current - self.last_uploaded
        self.last_uploaded = current
        self.__processed_bytes += chunk_size

    async def progress(self):
        self.total_time += self.update_interval

    async def _get_server(self):
        async with ClientSession() as session:
            async with session.get(f"{self.api_url}/get-server") as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}: {await resp.text()}")
                response = await resp.json()
                server = response.get("server")
                if not server:
                    raise Exception("VikingFile server response missing upload URL.")
                return server.rstrip("/")

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def _upload_file(self, server, file_path):
        file_name = ospath.basename(file_path)
        with ProgressFileReader(
            filename=file_path, read_callback=self.__progress_callback
        ) as file:
            data = FormData()
            data.add_field("file", file, filename=file_name)
            if self.user_hash:
                data.add_field("user", self.user_hash)

            async with ClientSession() as session:
                async with session.post(server, data=data) as resp:
                    if resp.status != 200:
                        raise Exception(f"HTTP {resp.status}: {await resp.text()}")
                    response = await resp.json(content_type=None)
                    link = response.get("url")
                    if not link:
                        raise Exception("VikingFile upload failed.")
                    return link

    async def _upload_directory(self):
        links = []
        server = await self._get_server()
        base_dir = ospath.basename(self._path)
        for root, _dirs, files in await sync_to_async(oswalk, self._path):
            if self.listener.is_cancelled:
                break
            for file in files:
                if self.listener.is_cancelled:
                    break
                file_path = ospath.join(root, file)
                link = await self._upload_file(server, file_path)
                self.total_files += 1
                rel_path = ospath.relpath(file_path, self._path)
                links.append((f"{base_dir}/{rel_path}", link))
        return links

    async def upload(self):
        try:
            LOGGER.info(f"VikingFile Uploading: {self._path}")
            self._updater = SetInterval(self.update_interval, self.progress)

            if not self.user_hash:
                raise ValueError(
                    "VikingFile user hash not configured! Please set it in user settings or config."
                )

            await self._upload_process()

        except Exception as err:
            if isinstance(err, RetryError):
                LOGGER.info(f"Total Attempts: {err.last_attempt.attempt_number}")
                err = err.last_attempt.exception()
            err = str(err).replace(">", "").replace("<", "")
            LOGGER.error(err)
            await self.listener.on_upload_error(err)
            self._is_errored = True
        finally:
            if self._updater:
                self._updater.cancel()
            if (
                self.listener.is_cancelled and not self._is_errored
            ) or self._is_errored:
                return

    async def _upload_process(self):
        if await aiopath.isfile(self._path):
            server = await self._get_server()
            link = await self._upload_file(server, self._path)
            mime_type = "File"
            self.total_files = 1
        elif await aiopath.isdir(self._path):
            uploaded = await self._upload_directory()
            if not uploaded:
                raise ValueError("No files uploaded from directory.")
            lines = [
                f"<a href='{link}'>{name}</a>" for name, link in uploaded if link
            ]
            page = await telegraph.create_page(
                title="VikingFile Uploads",
                content="<br>".join(lines),
            )
            link = f"https://telegra.ph/{page['path']}"
            mime_type = "Folder"
            self.total_folders = 1
        else:
            raise ValueError("Invalid file path!")

        if self.listener.is_cancelled:
            return

        LOGGER.info(f"Uploaded To VikingFile: {self.listener.name}")
        await self.listener.on_upload_complete(
            link,
            self.total_files,
            self.total_folders,
            mime_type,
            dir_id="",
        )

    async def cancel_task(self):
        self.listener.is_cancelled = True
        if self.is_uploading:
            LOGGER.info(f"Cancelling VikingFile Upload: {self.listener.name}")
            await self.listener.on_upload_error("VikingFile upload has been cancelled!")
