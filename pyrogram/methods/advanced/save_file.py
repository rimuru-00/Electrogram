from __future__ import annotations

import asyncio
import functools
import inspect
import io
import logging
import math
import time
from hashlib import md5
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, BinaryIO

import pyrogram
from pyrogram import StopTransmissionError, raw
from pyrogram.session import Session

if TYPE_CHECKING:
    from collections.abc import Callable

log = logging.getLogger(__name__)

SESSION_CACHE_EXPIRY = 21600

class SaveFile: 
    async def get_cached_sessions(
        self: pyrogram.Client,
        pool_size: int, 
        is_media: bool = True
    ) -> List[Session]:
        """Get cached sessions or create new ones if needed."""
        async with self.media_sessions_lock:
            dc_id = await self.storage.dc_id()
            auth_key = await self.storage.auth_key()
            test_mode = await self.storage.test_mode()
        
            cache_key = (dc_id, pool_size, is_media)
            if cache_key not in self.upload_sessions:
                self.upload_sessions[cache_key] = []
        
            current_time = time.time()
            cached_sessions = self.upload_sessions[cache_key]
            valid_sessions = []
        
            for session, timestamp in cached_sessions[:]:
                if current_time - timestamp < SESSION_CACHE_EXPIRY:
                    valid_sessions.append((session, timestamp))
                else:
                    self.loop.create_task(session.stop())
                    log.info(
                        "Closed expired session (age: %.2fs)", 
                        current_time - timestamp
                    )
                
            self.upload_sessions[cache_key] = valid_sessions
            available_sessions = [session for session, _ in valid_sessions[:pool_size]]
        
            new_sessions_count = pool_size - len(available_sessions)
            if new_sessions_count > 0:
                log.info(
                    "Creating %d new sessions for pool_size=%d", 
                    new_sessions_count, 
                    pool_size
                )
            
            while len(available_sessions) < pool_size:
                session = Session(
                    self,
                    dc_id,
                    auth_key,
                    test_mode,
                    is_media=is_media
                )
                available_sessions.append(session)
                self.upload_sessions[cache_key].append((session, current_time))
                await session.start()
            
            return available_sessions
    
    async def save_file(
        self: pyrogram.Client,
        path: str | BinaryIO,
        file_id: int | None = None,
        file_part: int = 0,
        progress: Callable | None = None,
        progress_args: tuple = (),
    ):
        """Upload a file onto Telegram servers, without actually sending the message to anyone.
        Useful whenever an InputFile type is required.

        .. note::
            This is a utility method intended to be used **only** when working with raw
            :obj:`functions <pyrogram.api.functions>` (i.e: a Telegram API method you wish to use which is not
            available yet in the Client class as an easy-to-use method).
        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            path (``str`` | ``BinaryIO``):
                The path of the file you want to upload that exists on your local machine or a binary file-like object
                with its attribute ".name" set for in-memory uploads.

            file_id (``int``, *optional*):
                In case a file part expired, pass the file_id and the file_part to retry uploading that specific chunk.

            file_part (``int``, *optional*):
                In case a file part expired, pass the file_id and the file_part to retry uploading that specific chunk.

            progress (``Callable``, *optional*):
                Pass a callback function to view the file transmission progress.
                The function must take *(current, total)* as positional arguments (look at Other Parameters below for a
                detailed description) and will be called back each time a new file chunk has been successfully
                transmitted.

            progress_args (``tuple``, *optional*):
                Extra custom arguments for the progress callback function.
                You can pass anything you need to be available in the progress callback scope; for example, a Message
                object or a Client instance in order to edit the message with the updated progress status.

        Other Parameters:
            current (``int``):
                The amount of bytes transmitted so far.

            total (``int``):
                The total size of the file.

            *args (``tuple``, *optional*):
                Extra custom arguments as defined in the ``progress_args`` parameter.
                You can either keep ``*args`` or add every single extra argument in your function signature.

        Returns:
            ``InputFile``: On success, the uploaded file is returned in form of an InputFile object.

        Raises:
            RPCError: In case of a Telegram RPC error.

        """
        if path is None:
            return None

        async def worker(session) -> None:
            while True:
                data = await queue.get()
 
                if data is None:
                    return
                for attempt in range(3):
                    try:
                        await session.invoke(data)
                        break
                    except Exception as e:
                        log.warning("Retrying part due to error: %s", e)
                        await asyncio.sleep(2**attempt)

        def create_rpc(chunk, file_part, is_big, file_id, file_total_parts):
            if is_big:
                return raw.functions.upload.SaveBigFilePart(
                    file_id=file_id,
                    file_part=file_part,
                    file_total_parts=file_total_parts,
                    bytes=chunk,
                )
            return raw.functions.upload.SaveFilePart(
                file_id=file_id,
                file_part=file_part,
                bytes=chunk,
            )

        part_size = 512 * 1024
        queue = asyncio.Queue(128)

        with (
            Path(path).open("rb", buffering=4096)  # noqa: ASYNC230
            if isinstance(path, str | PurePath)
            else path
        ) as fp:
            file_name = getattr(fp, "name", "file.jpg")
            fp.seek(0, io.SEEK_END)
            file_size = fp.tell()
            fp.seek(0)

            if file_size == 0:
                raise ValueError("File size equals to 0 B")

            file_size_limit_mib = 4000 if self.me.is_premium else 2000

            if file_size > file_size_limit_mib * 1024 * 1024:
                raise ValueError(
                    f"Can't upload files bigger than {file_size_limit_mib} MiB",
                )

            file_total_parts = math.ceil(file_size / part_size)
            is_big = file_size > 10 * 1024 * 1024
            pool_size = 2 if is_big else 1
            workers_count = 4 if is_big else 1
            is_missing_part = file_id is not None
            file_id = file_id or self.rnd_id()
            md5_sum = md5() if not is_big and not is_missing_part else None

            pool = await self.get_cached_sessions(pool_size, is_media=True)

            workers = [
                self.loop.create_task(worker(session))
                for session in pool
                for _ in range(workers_count)
            ]

            try:
                fp.seek(part_size * file_part)
                next_chunk_task = self.loop.create_task(self.preload(fp, part_size))

                while True:
                    chunk = await next_chunk_task
                    next_chunk_task = self.loop.create_task(
                        self.preload(fp, part_size),
                    )

                    if not chunk:
                        if not is_big and not is_missing_part:
                            md5_sum = md5_sum.hexdigest()
                        break

                    await queue.put(
                        create_rpc(
                            chunk,
                            file_part,
                            is_big,
                            file_id,
                            file_total_parts,
                        ),
                    )

                    if is_missing_part:
                        return None

                    if not is_big and not is_missing_part:
                        md5_sum.update(chunk)

                    file_part += 1

                    if progress:
                        func = functools.partial(
                            progress,
                            min(file_part * part_size, file_size),
                            file_size,
                            *progress_args,
                        )

                        if inspect.iscoroutinefunction(progress):
                            await func()
                        else:
                            await self.loop.run_in_executor(self.executor, func)
            except StopTransmissionError:
                raise
            except Exception as e:
                log.error(
                    "Error during file upload at part %s: %s",
                    file_part,
                    e,
                )
            else:
                if is_big:
                    return raw.types.InputFileBig(
                        id=file_id,
                        parts=file_total_parts,
                        name=file_name,
                    )
                return raw.types.InputFile(
                    id=file_id,
                    parts=file_total_parts,
                    name=file_name,
                    md5_checksum=md5_sum,
                )
            finally:
                for _ in workers:
                    await queue.put(None)
                await asyncio.gather(*workers)


    async def preload(self, fp, part_size):
        return fp.read(part_size)
        
    async def cleanup_cached_sessions(self: pyrogram.Client):
        """Clean up expired sessions from the cache."""
        current_time = time.time()
        for cache_key, sessions in list(self.upload_sessions.items()):
            valid_sessions = []
            for session, timestamp in sessions:
                if current_time - timestamp < SESSION_CACHE_EXPIRY:
                    valid_sessions.append((session, timestamp))
                else:
                    try:
                        await session.stop()
                        log.debug("Closed expired session during cleanup")
                    except Exception as e:
                        log.warning("Error closing expired session: %s", e)
            
            if valid_sessions:
                self.upload_sessions[cache_key] = valid_sessions
            else:
                del self.upload_sessions[cache_key]
