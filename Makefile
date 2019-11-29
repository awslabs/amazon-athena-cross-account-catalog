LAMBDA_FUNCTION_NAME=cross-account-athena

build:
	[ ! -d target ] && mkdir target || echo
	[ ! -d package ] && mkdir package || echo
	pip install -r requirements.txt --target ./package
	cd package && zip -r9 ../target/functionv2.zip .
	zip -r -x "*/__pycache__/*" -g target/functionv2.zip heracles

clean:
	rm -rf ./package/* ./target/*

update:
	aws lambda update-function-code --function-name ${LAMBDA_FUNCTION_NAME} --zip-file fileb://target/functionv2.zip