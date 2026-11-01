"""Synthetic HTTP fixture. Run only in a disposable, network-isolated container."""
import sys


def main():
    sys.path.insert(0, "/opt/hue-emulator")
    sys.argv = [
        "HueEmulator3.py",
        "--config_path", "/tmp/diyhue-scene-events-config",
        "--mac", "02:00:00:00:00:01",
        "--ip", "127.0.0.1",
        "--bind-ip", "127.0.0.1",
        "--no-serve-https",
    ]
    from HueEmulator3 import app
    from HueObjects.ApiUser import ApiUser
    from flask import request
    from flaskUI import v2restapi
    import configManager

    configManager.bridgeConfig.yaml_config["apiUsers"]["scene-delete-event-test-key"] = ApiUser(
        "scene-delete-event-test-key", "Synthetic scene event test", None
    )
    retained = {}

    @app.post("/testing/retain")
    def retain():
        data = request.get_json()
        key = (data["type"], data["id"])
        target = v2restapi.getObject(*key)
        assert target is not None
        retained[key] = target
        return {"retained": True}

    @app.post("/testing/release")
    def release():
        retained.clear()
        return {"retained": False}

    @app.post("/testing/persist")
    def persist():
        configManager.bridgeConfig.save_config(backup=False, blocking=True)
        return {"save_call": "returned"}

    app.run(host="127.0.0.1", port=8080, use_reloader=False)


if __name__ == "__main__":
    main()
