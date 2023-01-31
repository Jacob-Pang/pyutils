import asyncio
import json
import os
import re
import requests

from collections import defaultdict
from subprocess import Popen, PIPE
from telethon import TelegramClient, Button, events
from threading import Thread
from .websurfer.rpa.manager import rpa_manager

def send_telegram_message(bot_token: str, chat_id: str, message: str) -> None:
    for _ in range(99):
        try:
            return requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": message, "parse_mode": "html"})
        except:
            pass
    
    requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "html"})

def make_bot_from_config(config_json: str) -> "CommandBotBase":
    with open(config_json) as json_file:
        config = json.load(json_file)

    name = config.get("name") if "name" in config else "CommandBot"
    shortcuts = config.get("shortcuts") if "shortcuts" in config else dict()

    client = TelegramClient(name, int(config.get("api_id")), config.get("api_hash"))
    return CommandBotBase(client, config.get("bot_token"), name=name, shortcuts=shortcuts)

def run_command_bot(command_bot: "CommandBotBase") -> None:
    @command_bot.client.on(events.NewMessage(pattern="/(?i)"))
    async def run_message_event_handler(event):
        await command_bot.command_event_handler(event)

    @command_bot.client.on(events.NewMessage(func=lambda event: event.geo))
    async def receive_location_handler(event):
        await command_bot.receive_location_handler(event)

    command_bot.start_client()
    command_bot.client.run_until_disconnected()


class CommandBotBase:
    @staticmethod
    def parse_process_flags(flags: dict[str, any], args_text: str) -> str:
        for flag in flags:
            if args_text.startswith(flag):
                args_text = args_text.removeprefix(flag).strip()
                flag_value, args_text = args_text.split(' ', maxsplit=1)

                flags[flag] = flag_value
                return CommandBotBase.parse_process_flags(flags, args_text.strip())

        return args_text

    @staticmethod
    def flush_py_cmd(args_text: str) -> str:
        args_text = re.sub(r"(^|\s)(py\s)", r"\1\2-u ", args_text)
        args_text = re.sub(r"(^|\s)(python\s)", r"\1\2-u ", args_text)

        return args_text

    def __init__(self, client: TelegramClient, bot_token: str, name: str = "CommandBot",
        shortcuts: dict[str, str] = dict()):

        self.client = client
        self.bot_token = bot_token
        self.name = name
        self.shortcuts = shortcuts
        self.request_location_futures = dict[int, asyncio.Future]()

        # Tracks mapping of process_alias to (process, output_bot_token, output_chat_id)
        self.processes = dict[str, tuple[Popen, str, int]]()

        # Tracks mapping of sender_id to process_alias of active (tracked) processes
        self.active_processes = defaultdict(list[str])

    # Initializer
    def start_client(self):
        self.client.start(bot_token=self.bot_token)

    # Process managers
    def pipe_process_outputs(self, process_alias: str, edit_message: events.NewMessage = None):
        process, output_bot_token, output_chat_id = self.processes[process_alias]

        for output in process.stdout:
            try:    output = output.decode('utf-8')
            except: output = f"(undecoded) {output}"

            output = output.replace('\r', '\n')
            send_telegram_message(
                output_bot_token, output_chat_id,
                f"<b>Process [{process_alias}]</b>:\n{output}"
            )

        process.wait()
        self.kill_process(process_alias) # Does not invoke Popen.terminate
    
    def pipe_process_errors(self, process_alias: str):
        process, output_bot_token, output_chat_id = self.processes[process_alias]

        for output in process.stderr:
            try:    output = output.decode('utf-8')
            except: output = f"(undecoded) {output}"

            output = output.replace('\r', '\n')
            send_telegram_message(
                output_bot_token, output_chat_id,
                f"<b>Process [{process_alias}] Error</b>:\n{output}"
            )

    def kill_process(self, process_alias: str):
        if process_alias not in self.processes:
            return

        process, output_bot_token, output_chat_id = self.processes[process_alias]
        status = process.poll()

        if status is None: # Busy process
            process.terminate()

        self.processes.pop(process_alias)

        # Remove process_alias from tracked active processes
        for user in self.active_processes.keys():
            if process_alias in self.active_processes[user]:
                self.active_processes[user].remove(process_alias)

        send_telegram_message(
            output_bot_token, output_chat_id,
            f"<b>Process [{process_alias}]</b> closed."
        )

    # Commands
    async def help(self, event: events.NewMessage.Event):
        await self.echo(event,
            "<b>Keyword List:</b>\n" +
            "Keywords are substituted at runtime and use the format <i>%keyword%</i>.\n" +
            "<i>location</i>\n" +
            "   Returns <i>-longitude longtitude -latitude latitude -access_hash access_hash</i>\n"
            "\n<b>Command List:</b>\n" +
            "<i>/echo</i>\n" +
            "   Repeats the message contents (keywords are broken down).\n" +
            "<i>/cmd -alias name -cwd dpath</i>\n" +
            "   Opens an asynchronous command shell.\n" +
            "<i>/execute -alias name -cwd dpath command</i>\n" +
            "   Executes the command asynchronously."
        )

    async def disconnect(self, event: events.NewMessage.Event):
        await self.echo(event, f"{self.name} disconnected.")
        await self.client.disconnect()

    async def echo(self, event: events.NewMessage.Event, message: str, sender_id: int = None) -> None:
        if not sender_id:
            sender = await event.get_sender()
            sender_id = sender.id
        
        await self.client.send_message(sender_id, message, parse_mode="HTML")

    async def make_shortcut(self, event: events.NewMessage.Event, command: str) -> None:
        shortcut, command = command.split(' ', maxsplit=1)
        shortcut = shortcut.strip()
        command = command.strip()

        if not command.startswith('/'):
            command = '/' + command
        
        self.shortcuts[shortcut] = command
        await self.echo(event, f"shortcut [/{shortcut} -> {command}] created.")

    async def rm_shortcut(self, event: events.NewMessage.Event, shortcut: str) -> None:
        if shortcut in self.shortcuts:
            return self.shortcuts.pop(shortcut)

        await self.echo(event, f"<b>Error:</b> shortcut [{shortcut}] not found.")

    async def cmd(self, event: events.NewMessage.Event, args_text: str = None) -> None:
        if args_text:
            await self.execute(event, f"{args_text} cmd.exe")
        else:
            await self.execute(event, "cmd.exe")

    async def execute(self, event: events.NewMessage.Event, args_text: str) -> None:
        chat = await event.get_chat()
        sender = await event.get_sender()
        sender_id = sender.id

        # Parse /execute flags
        flags = {
            "-alias": None,
            "-output_bot_token": self.bot_token,
            "-output_chat_id": chat.id,
            "-cwd": os.getcwd()
        }

        args_text = self.parse_process_flags(flags, args_text)
        args_text = self.flush_py_cmd(args_text)

        process = Popen(args_text, stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=flags.get("-cwd"))
        process_alias = flags.get("-alias") if flags.get("-alias") else str(process.pid)
        self.processes[process_alias] = (process, flags["output_bot_token"],
                flags["output_chat_id"])

        await self.echo(event, f"<b>Process [{process_alias}]</b> started and active.",
                sender_id=sender_id)

        Thread(target=self.pipe_process_errors, args=(process_alias,)).start()
        Thread(target=self.pipe_process_outputs, args=(process_alias,)).start()

        self.active_processes[sender_id].append(process_alias)

    async def close(self, event: events.NewMessage.Event, process_alias: str = None) -> None:
        chat = await event.get_chat()
        chat_id = chat.id

        active_process_alias = self.active_processes[chat_id][-1] \
            if self.active_processes[chat_id] else None

        if not process_alias: # Assign to active process
            process_alias = active_process_alias
        
        if not process_alias:
            await self.echo(event, f"<b>Error:</b> No active process found.")
            return

        self.kill_process(process_alias)

    async def activate(self, event: events.NewMessage.Event, process_alias: str) -> None:
        # Activates the process (sets to current active process)
        sender = await event.get_sender()
        sender_id = sender.id

        if process_alias not in self.processes:
            return await self.echo(event, f"<b>Error:</b> process alias [{process_alias}] not recognized.",
                    sender_id=sender_id)
        
        if process_alias in self.active_processes[sender_id]:
            if process_alias == self.active_processes[sender_id][-1]:
                return # Currently the active process

            self.active_processes[sender_id].remove(process_alias)

        self.active_processes[sender_id].append(process_alias)

    async def input(self, event: events.NewMessage.Event, pipe_input: str) -> None:
        # Pipes the input to the active process
        sender = await event.get_sender()
        sender_id = sender.id

        if not self.active_processes[sender_id]:
            await self.echo(event, f"<b>Error:</b> No active process found.", sender_id=sender_id)
        else:
            pipe_input = self.flush_py_cmd(pipe_input)
            process_alias = self.active_processes[sender_id][-1]
            process, _, _ = self.processes[process_alias]
            process.stdin.write(f"{pipe_input}\r\n".encode("utf-8"))
            process.stdin.flush()

    async def request_location(self, event: events.NewMessage.Event) -> None:
        sender = await event.get_sender()
        sender_id = sender.id

        event_loop = asyncio.get_running_loop()
        self.request_location_futures[sender_id] = event_loop.create_future()

        await self.client.send_message(
            sender_id, "Requesting for current location ...",
            buttons=[ Button.request_location("share current location", single_use=1) ]
        )

    # Event handlers
    async def receive_location_handler(self, event: events.NewMessage.Event):
        sender = await event.get_sender()
        sender_id = sender.id

        if not sender_id in self.request_location_futures:
            return

        self.request_location_futures[sender_id].set_result(event.geo)
        await self.client.send_message(sender_id, "Location updated.", buttons=Button.clear())

    async def keyword_decay_handler(self, event: events.NewMessage.Event, args_text: str) -> str:
        if r"%location%" in args_text:
            sender = await event.get_sender()
            sender_id = sender.id

            if not sender_id in self.request_location_futures:
                await self.request_location(event)

            geopoint = await self.request_location_futures[sender_id]
            args_text = args_text.replace(r"%location%", f"-longitude {geopoint.long} "
                    + f"-latitude {geopoint.lat} -access_hash {geopoint.access_hash}")

            self.request_location_futures.pop(sender_id) # Consume futures
        
        if r"%rpa_instance_id%" in args_text:
            rpa_instance_id = rpa_manager.assign_rpa_instance_id()
            args_text = args_text.replace(r"%rpa_instance_id%", f"-rpa_instance_id {rpa_instance_id}")

        if r"%rpa_config%" in args_text:
            args_text = args_text.replace(r"%rpa_config%",
                f"-locking_files_dpath {rpa_manager.locking_files_dpath} " +
                f"-cloned_module_dpath {rpa_manager.cloned_module_dpath} " +
                f"-cloned_source_dpath {rpa_manager.cloned_source_dpath} "
            )

        return args_text

    async def command_event_handler(self, event: events.NewMessage.Event):
        async def command_handler(command: str):
            _, command = command.split('/', maxsplit=1)
            command = command.strip()
            args_text = ""
            
            if ' ' in command:
                # command has args
                command_name, args_text = command.split(' ', maxsplit=1)
                command_name = command_name.strip()
                args_text = args_text.strip()
            else:
                command_name = command

            try: # Execute command wrapper
                if command_name in self.shortcuts:
                    return await command_handler(f"{self.shortcuts.get(command_name)} {args_text}")
                elif "handler" in command_name:
                    return await self.echo(event, f"<b>Error:</b> cannot invoke handler commands.")
                elif not hasattr(self, command_name):
                    sender = await event.get_sender()
                    sender_id = sender.id

                    if command_name in self.active_processes[sender_id]:
                        # /activate-input shortcut
                        await self.activate(event, command_name)
                        command_name = "input"
                    else:
                        return await self.echo(event, f"<b>Error:</b> command [{command_name}]"
                                + " not recognized.")
                
                if command_name != "make_shortcut":
                    args_text = await self.keyword_decay_handler(event, args_text)

                if args_text:
                    await getattr(self, command_name)(event, args_text)
                else:
                    await getattr(self, command_name)(event)
            except Exception as exception:
                await self.echo(event, f"<b>Exception encountered:</b>\n{exception}")

        # Parse command
        await command_handler(str(event.raw_text))

if __name__ == "__main__":
    pass