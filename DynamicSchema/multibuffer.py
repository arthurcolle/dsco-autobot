class MultiBuffer:
    def __init__(self):
        self.buffers = {}

    def add_buffer(self, name):
        if name not in self.buffers:
            self.buffers[name] = []

    def write_to_buffer(self, name, data):
        if name in self.buffers:
            self.buffers[name].append(data)
        else:
            raise ValueError(f"Buffer '{name}' does not exist")

    def read_from_buffer(self, name):
        if name in self.buffers:
            return ''.join(self.buffers[name])
        else:
            raise ValueError(f"Buffer '{name}' does not exist")

    def clear_buffer(self, name):
        if name in self.buffers:
            self.buffers[name] = []
        else:
            raise ValueError(f"Buffer '{name}' does not exist")

if __name__ == "__main__":
    # Example usage
    mb = MultiBuffer()
    mb.add_buffer("buffer1")
    mb.write_to_buffer("buffer1", "Hello, ")
    mb.write_to_buffer("buffer1", "World!")
    print(mb.read_from_buffer("buffer1"))  # Output: Hello, World!
    mb.clear_buffer("buffer1")
    print(mb.read_from_buffer("buffer1"))  # Output: (empty string)
