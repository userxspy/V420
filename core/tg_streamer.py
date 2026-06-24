import logging
from hydrogram import Client, raw, utils
from hydrogram.file_id import FileId, FileType, ThumbnailSource
from hydrogram.session import Session, Auth
from hydrogram.errors import AuthBytesInvalid
from utils import temp

logger = logging.getLogger(__name__)

class SmartStreamer:
    """
    Fast Finder Stream Engine:
    यह क्लास Telegram सर्वर से सीधा संपर्क करती है और वीडियो के बाइट्स (Bytes) को
    बिना सर्वर की RAM भरे सीधे यूज़र के ब्राउज़र तक पहुँचाती है (Dynamic Chunking)।
    """

    async def get_file_properties(self, msg) -> FileId:
        """मैसेज से फाइल की प्रॉपर्टीज (File ID) निकालता है"""
        return FileId.decode(getattr(msg, msg.media.value).file_id)

    async def generate_media_session(self, c: Client, d: FileId) -> Session:
        """Telegram के डेटा-सेंटर (DC) के साथ स्ट्रीमिंग सेशन बनाता है"""
        ms = c.media_sessions.get(d.dc_id)

        if not ms:
            test_mode = await c.storage.test_mode()
            if d.dc_id != await c.storage.dc_id():
                ms = Session(
                    c, d.dc_id,
                    await Auth(c, d.dc_id, test_mode).create(),
                    test_mode, is_media=True
                )
                await ms.start()
                for _ in range(3):
                    try:
                        ex = await c.invoke(raw.functions.auth.ExportAuthorization(dc_id=d.dc_id))
                        await ms.send(raw.functions.auth.ImportAuthorization(id=ex.id, bytes=ex.bytes))
                        break
                    except AuthBytesInvalid:
                        continue
                else:
                    await ms.stop()
                    raise AuthBytesInvalid
            else:
                ms = Session(c, d.dc_id, await c.storage.auth_key(), test_mode, is_media=True)
                await ms.start()
            c.media_sessions[d.dc_id] = ms

        return ms

    async def get_location(self, f: FileId):
        """फाइल के प्रकार के अनुसार लोकेशन जनरेट करता है"""
        if f.file_type == FileType.CHAT_PHOTO:
            if f.chat_id > 0:
                peer = raw.types.InputPeerUser(user_id=f.chat_id, access_hash=f.chat_access_hash)
            elif f.chat_access_hash == 0:
                peer = raw.types.InputPeerChat(chat_id=-f.chat_id)
            else:
                peer = raw.types.InputPeerChannel(
                    channel_id=utils.get_channel_id(f.chat_id),
                    access_hash=f.chat_access_hash
                )
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer, volume_id=f.volume_id, local_id=f.local_id,
                big=f.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG
            )
        elif f.file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=f.media_id, access_hash=f.access_hash,
                file_reference=f.file_reference, thumb_size=f.thumbnail_size
            )
        return raw.types.InputDocumentFileLocation(
            id=f.media_id, access_hash=f.access_hash,
            file_reference=f.file_reference, thumb_size=f.thumbnail_size
        )

    # 🚀 SMART DYNAMIC CHUNKING (Zero RAM Overhead)
    async def stream_file(self, msg, start_byte: int, end_byte: int):
        file_props = await self.get_file_properties(msg)
        
        # ✅ FIX: यहाँ self.main_bot की जगह सीधा temp.BOT का इस्तेमाल किया गया है
        ms = await self.generate_media_session(temp.BOT, file_props)
        
        loc = await self.get_location(file_props)

        current_offset = start_byte
        chunk_size = 1024 * 128 
        max_chunk = 1024 * 1024 

        try:
            while current_offset <= end_byte:
                aligned_offset = current_offset - (current_offset % 4096)
                bytes_needed = end_byte - current_offset + 1
                
                limit = min(chunk_size, bytes_needed + (current_offset - aligned_offset))
                aligned_limit = min(((limit + 4095) // 4096) * 4096, 1048576)

                r = await ms.send(
                    raw.functions.upload.GetFile(
                        location=loc, offset=aligned_offset, limit=aligned_limit
                    )
                )
                
                if not isinstance(r, raw.types.upload.File) or not r.bytes:
                    break

                start_cut = current_offset - aligned_offset
                end_cut = start_cut + min(bytes_needed, len(r.bytes) - start_cut)
                
                chunk = r.bytes[start_cut:end_cut]
                if not chunk:
                    break

                yield chunk
                current_offset += len(chunk)
                
                if chunk_size < max_chunk:
                    chunk_size = min(max_chunk, chunk_size * 2)
                    
        except Exception as e:
            logger.error(f"Stream interrupted at byte {current_offset}: {e}")
