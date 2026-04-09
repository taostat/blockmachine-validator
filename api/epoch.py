from validator.config import EpochConfig


class EpochCalculator:
    def __init__(self, config: EpochConfig):
        self.config = config

    def get_current_epoch(self, current_block: int) -> int:
        adjusted = current_block - self.config.start_block - self.config.buffer_blocks
        if adjusted < 0:
            return -1
        return (adjusted // self.config.epoch_length_blocks) - 1

    def get_epoch_id(self, epoch_number: int) -> str:
        start = self.config.start_block + (
            epoch_number * self.config.epoch_length_blocks
        )
        return str(start)

    def epoch_id_to_number(self, epoch_id: str) -> int:
        try:
            start_block = int(epoch_id)
            return (
                start_block - self.config.start_block
            ) // self.config.epoch_length_blocks
        except ValueError:
            return 0
