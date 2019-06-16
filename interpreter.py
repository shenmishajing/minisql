import API


def print_welcome():
    print('Welcome to the MySQL monitor.  Commands end with ; or \\g.')
    print('You can use it like MySQL with less command')
    print("Type 'help;' or '\\h' for help. Type '\\c' to clear the current input statement.")


def get_command():
    command = ''
    print('minisql> ', end='')
    string = input()
    while True:
        if ';' in string:
            command += string[:string.find(';')]
            break
        command += string
        print('      -> ', end='')
        string = input()
    return command


def main():
    api = API.API(4 * 1024, 4 * 1024)
    print_welcome()
    command = ''
    while command != 'quit':
        command = get_command()
        print(command)
        print(api.exec_sql(command))


if __name__ == '__main__':
    main()
