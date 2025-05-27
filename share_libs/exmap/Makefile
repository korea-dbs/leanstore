all: module eval

module:
	make -C module/

eval:
	make -C eval/

clean:
	cd module && make clean

.PHONY: module eval clean
