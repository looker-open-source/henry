# color.py


# string is the original string
# type determines the color/label added
# style can be color or text
class color(object):
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

    def format(self, string, type, style='color'):
        formatted_string = ''
        string = str(string)
        if style == 'color':
            if type in ('success', 'pass'):
                formatted_string += self.GREEN + string + self.ENDC
            elif type == 'warning':
                formatted_string += self.WARNING + string + self.ENDC
            elif type in ('error', 'fail'):
                formatted_string += self.FAIL + string + self.ENDC
        elif style == 'text':
            if type == 'success':
                formatted_string += string
            elif type == 'warning':
                formatted_string += 'WARNING: ' + string
            elif type == 'error':
                formatted_string += 'ERROR: ' + string

        return formatted_string
