import argparse

# Custom libraries:
from include import utils
from include import rpc
from include import globals
import settings


def main(args):
    utils.vprint("invoking RPC command against %s daemon" % (args.type))
    # @TODO: support parameters

    method = rpc.methods[args.subparser_name]
    parms = []
    for parameter in method['parameters']:
        parm = getattr(args, parameter['parameter'])
        if parm is not None:
          parms.append(parm)
    response = rpc.invoke_method(method=args.subparser_name, parameters=parms)

    utils.vprint("error: %s" % (response['error'],))
    print(response['result'])

if __name__ == '__main__':
    globals.init()
    globals.settings = settings
    parser = argparse.ArgumentParser(description="Coin daemon CLI")
    parser.add_argument('--host', help="coin daemon host and port, (for example: 'https://localhost:8332')", type=str)
    parser.add_argument('-t', '--type', help="coin type to extract", type=str, choices=utils.supported_coins(settings), required=True)
    parser.add_argument('-v', dest='verbose', action='count', help="verbose output")
    subparsers = parser.add_subparsers(dest='subparser_name', help="RPC COMMANDS")
    for argument in rpc.methods.keys():
        subparser = subparsers.add_parser(argument, help=rpc.methods[argument]['description'])
        for parameter in rpc.methods[argument]['parameters']:
            if parameter['format'] == 'int':
                format = int
            elif parameter['format'] == 'bool':
                format = bool
            elif parameter['format'] == 'float':
                format = float
            elif parameter['format'] == 'array':
                format = list
            elif parameter['format'] == 'object':
                format = object
            else:
                format = str
            subparser.add_argument("--" + parameter['parameter'], help=parameter['description'], type=format, required=parameter['required'])
    globals.args = parser.parse_args()
    utils.vprint("starting ...")
    rc = main(globals.args)
    utils.vprint("done!")
    exit(rc)
