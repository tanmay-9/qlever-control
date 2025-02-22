from __future__ import annotations

from importlib import import_module
from pathlib import Path

from qlever import snake_to_camel
from qlever.command import QleverCommand


class CommandObjects:

    def __init__(self, script_name: str) -> None:
        self.script_name = script_name
        self.engine_name = script_name[1:]
        self._command_objects = self._fetch_command_objects()

    def _fetch_command_objects(self) -> dict[str, QleverCommand]:
        command_objects = {}
        package_path = Path(__file__).parent.parent / self.script_name
        command_names = [
            Path(p).stem
            for p in package_path.glob("commands/*.py")
            if p.name != "__init__.py"
        ]
        for command_name in command_names:
            module_path = f"{self.script_name}.commands.{command_name}"
            class_name = snake_to_camel(command_name) + "Command"
            try:
                module = import_module(module_path)
            except ImportError as e:
                raise Exception(
                    f"Could not import module {module_path} "
                    f"for engine {self.engine_name}: {e}"
                ) from e
            # Create an object of the class and store it in the dictionary. For the
            # commands, take - instead of _.
            command_class = getattr(module, class_name)
            command_objects[command_name.replace("_", "-")] = command_class()
        return command_objects

    def __iter__(self):
        return iter(self._command_objects.items())

    def __getitem__(self, command: str) -> QleverCommand:
        return self._command_objects[command]
