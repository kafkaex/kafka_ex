#!/usr/bin/awk -f
# print only active ifaces from ifconfig
#    from http://unix.stackexchange.com/questions/103241/how-to-use-ifconfig-to-show-active-interface-only
#    (using a version without pcregrep because that may not be available)
BEGIN            { print_it = 0  }
/status: active/ { print_it = 1  }
/^($|[^\t])/     { if(print_it) print buffer; buffer = $0; print_it = 0  }
/^\t/            { buffer = buffer "\n" $0  }
END              { if(print_it) print buffer  }
