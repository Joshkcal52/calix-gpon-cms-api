import logging
from app.models.hp_model import HPInterfaceList, HPInterface
from app.controller.ssh_controller import ssh_controller

logger = logging.getLogger(__name__)


class HPController:
    async def get_interfaces(self) -> HPInterfaceList:
        # Run the command
        response = await ssh_controller.execute_command("show interface")
        # Parse the output
        interfaces = self._parse_show_interface(response.output)
        return HPInterfaceList(interfaces=interfaces)

    def _parse_show_interface(self, output: str) -> list[HPInterface]:
        """
        Parses the output of 'show interface' to extract port information.

        Args:
            output: The raw string output from the switch command.

        Returns:
            A list of HPInterface objects.
        """
        interfaces = []
        lines = output.splitlines()
        data_started = False

        logger.info(f"Total lines to process: {len(lines)}")

        for i, line in enumerate(lines):
            if "----------" in line:
                data_started = True
                logger.info(f"Found separator at line {i}: {line}")
                continue
            if not data_started:
                continue
            if not line.strip():
                continue  # Just skip blank lines, don't break

            parts = line.split()
            logger.info(f"Line {i}: '{line}' -> parts: {parts}")

            # Only process lines where the first part looks like a port (number or A1, etc.)
            if len(parts) >= 7 and (parts[0].isdigit() or parts[0][0].isalpha()):
                try:
                    total_bytes = parts[1].replace(',', '')
                    total_frames = parts[2].replace(',', '')
                    errors_rx = int(parts[3])
                    drops_tx = int(parts[4])
                    interfaces.append(HPInterface(
                        name=parts[0],
                        total_bytes=total_bytes,
                        total_frames=total_frames,
                        errors_rx=errors_rx,
                        drops_tx=drops_tx,
                        status=None,
                        speed=None
                    ))
                    logger.info(f"Successfully parsed interface: {parts[0]}")
                except Exception as e:
                    logger.warning(f"Failed to parse line: {line} ({e})")
            else:
                logger.info(f"Skipping line {i}: len(parts)={len(parts)}, first_part='{parts[0] if parts else 'N/A'}'")

        logger.info(f"Total interfaces parsed: {len(interfaces)}")
        return interfaces


hp_controller = HPController()
