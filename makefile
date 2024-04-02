
.ONESHELL:
.SILENT:


test:
	/.pyenv/versions/3.11.*/bin/python3.11 -m pytest -s --disable-warnings

base_image_nogpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="base-image-nogpu";

image_nogpu:
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="image-nogpu"

base_image_gpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="base-image-gpu";

image_gpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="image-gpu";

dev:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=default-image \
			--location=us \
			--repository=burla-subjob-images/default \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-subjob-images/default/image-nogpu \
	); \
	echo $${IMAGE_NAME}; \
	docker run --rm -it \
		-v $(PWD):/burla \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=burla-test \
		-e IN_DEV=True \
		-p 5001:5000 \
		$${IMAGE_NAME} bash
