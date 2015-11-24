#!/usr/bin/env python

import sys
import time
import imp
# import claim_pb2


def contains_strings(prop, claims):
    for claim in claims:
        val = getattr(claim, prop)
        if len(str(val)) == 0:
            return True
        try:
            float(val)
        except:
            return True
    return False


def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print '%s function took %0.3f s' % (f.func_name, (time2-time1))
        return ret
    return wrap


@timing
def convert(pb_file):
    with open(pb_file, 'rb') as f:
        claims = claim_pb2.ProtoListClaim()
        claims.ParseFromString(f.read())
        print "Number of rows:", len(claims.Claims)

        if len(claims.Claims) == 0:
            return

        props = []
        for property, value in vars(claims.Claims[0].__class__).iteritems():
            if ("_FIELD_NUMBER" not in property
                    and property[0] != "_" and property != "DESCRIPTOR"
                    and not callable(getattr(claims.Claims[0], property))):
                props.append(property)

        print "Number of columns:", len(props)

        cl = claims.Claims

        i = 0
        out = open("out.Rda", "w")
        out.write("structure(list(\n\n")
        for prop in props:
            # print i, prop
            out.write("o%03d = " % i)

            if contains_strings(prop, cl):
                # write strings
                #out.write("structure(1:%d, .Label = c(" % len(cl))
                #vals = ["\"%s\"" % str(getattr(x, prop)).replace("\"", "'") for x in cl]
                #out.write(", ".join(vals))
                #out.write("), class = \"character\")")
                out.write("c(")
                out.write(", ".join(['"%s"' % getattr(x, prop).replace("\"", "'") for x in cl]))
                out.write(")")
            else:
                # write numbers
                out.write("c(")
                out.write(", ".join([str(getattr(x, prop)) for x in cl]))
                out.write(")")

            if i+1 != len(props):
                out.write(",\n")
            else:
                out.write("),\n")
            i += 1

        out.write("\n.Names = c(")
        out.write(", ".join(["\"%s\"" % x for x in props]))
        out.write("), row.names = c(NA, -%dL), class = \"data.frame\")" % len(cl))
        out.close()


if __name__ == "__main__":
    file = sys.argv[1]
    proto = sys.argv[2]
    claim_pb2 = imp.load_source('claim_pb2', proto)
    convert(file)
