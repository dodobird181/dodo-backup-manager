import io
import os
from typing import Any

import pytest

# @pytest.mark.django_db
# @pytest.mark.parametrize(
#     "image, save_name",
#     [
#         (get_test_image("test_image.png"), "compressed_test_image.webp"),
#         (get_test_image("test_image.jpg"), "compressed_test_image.webp"),
#         (get_test_image("test_image.jpeg"), "compressed_test_image.webp"),
#     ],
# )
# def test_create_style_compresses_marker_icon_images(image, save_name: str):
#     kwargs = create_style_kwargs() | {"marker_icon": image}
#     style = Style.objects.create(**kwargs)

#     assert os.path.exists(style.marker_icon.path)
#     assert style.marker_icon.name.endswith(save_name)
#     assert style.marker_icon.size < image.size  # compressed image should be smaller


def test_hello():
    assert True
