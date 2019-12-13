from redbot.core import Config
from redbot.core.bot import Red

from .audio_dataclasses import _pass_config_to_dataclasses
from .apis import _pass_config_to_api
from .converters import _pass_config_to_converters
from .playlists import _pass_config_to_playlist
from .utils import _pass_config_to_utils


def pass_config_to_dependencies(config: Config, bot: Red, localtracks_folder: str):
    _pass_config_to_api(config)
    _pass_config_to_converters(config, bot)
    _pass_config_to_playlist(config, bot)
    _pass_config_to_dataclasses(config, bot, localtracks_folder)
    _pass_config_to_utils(config, bot)
