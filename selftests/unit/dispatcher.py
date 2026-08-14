import unittest
from unittest.mock import MagicMock, patch

from avocado.core.dispatcher import EnabledExtensionManager
from avocado.core.extension_manager import PluginPriority
from avocado.core.settings import settings


class DispatcherTest(unittest.TestCase):
    def test_order(self):
        """
        Simply checks that the default order is based on the extension names
        """
        namespaces = [
            ("avocado.plugins.cli", {}),
            ("avocado.plugins.cli.cmd", {}),
            ("avocado.plugins.job.prepost", {}),
            ("avocado.plugins.result", {}),
            ("avocado.plugins.resolver", {"config": None}),
        ]
        for namespace in namespaces:
            with self.subTest(i=namespace):
                namespace, invoke_kwds = namespace
                ext_objects = EnabledExtensionManager(namespace, invoke_kwds).extensions
                sort = sorted(ext_objects, key=lambda x: x.name)
                sort = sorted(
                    sort,
                    key=lambda x: getattr(x.obj, "priority", PluginPriority.NORMAL),
                    reverse=True,
                )
                self.assertEqual(ext_objects, sort)

    def test_disabled_plugin_is_not_imported(self):
        entry_point = MagicMock()
        entry_point.name = "disabled"
        config = {
            "plugins.cli.order": [],
            "plugins.disable": ["cli.disabled"],
        }
        with (
            patch(
                "avocado.core.extension_manager.get_entry_points_for",
                return_value=[entry_point],
            ),
            patch.object(settings, "as_dict", return_value=config),
        ):
            manager = EnabledExtensionManager("avocado.plugins.cli")

        entry_point.load.assert_not_called()
        self.assertEqual(manager.extensions, [])


if __name__ == "__main__":
    unittest.main()
