from pathlib import Path
import tempfile
import unittest

import scripts.check_paper_only as paper_only


def scan_repository(root: Path):
    scan = getattr(paper_only, "scan_repository", None)
    assert callable(scan), "scan_repository API is not implemented"
    return scan(root)


def write_public_file(root: Path, relative_path: str, body: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


class PaperOnlyScannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_rejects_wallet_secret_and_order_submission(self) -> None:
        write_public_file(
            self.root,
            "crates/probe/src/bad.rs",
            'let private_key = env::var("PRIVATE_KEY")?;\n'
            "client.submit_order(order).await?;\n",
        )

        findings = scan_repository(self.root)

        self.assertTrue(
            any(
                finding.path == "crates/probe/src/bad.rs"
                and finding.line == 1
                and finding.reason == "credential or signer capability"
                for finding in findings
            )
        )
        self.assertTrue(
            any(
                finding.path == "crates/probe/src/bad.rs"
                and finding.line == 2
                and finding.reason == "external order-submission capability"
                for finding in findings
            )
        )

    def test_allows_paper_fill_simulation(self) -> None:
        write_public_file(
            self.root,
            "crates/probe/src/paper.rs",
            "let simulated_fill = paper_engine.fill(decision);\n",
        )

        self.assertEqual(scan_repository(self.root), [])

    def test_public_copy_defines_execution_as_simulation(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        self.assertTrue(readme.startswith("# Polymarket Pricing Research"))
        self.assertIn("paper-fill simulation", readme)
        self.assertIn("no external order-submission path", readme)

    def test_private_candidate_pages_deploy_is_manual_only(self) -> None:
        workflow = Path(".github/workflows/pages.yml").read_text(
            encoding="utf-8"
        )
        trigger_lines: list[str] = []
        inside_triggers = False

        for line in workflow.splitlines():
            if line == "on:":
                inside_triggers = True
                continue
            if inside_triggers and line and not line.startswith((" ", "\t")):
                break
            if inside_triggers:
                trigger_lines.append(line)

        triggers = {
            line.strip().removesuffix(":")
            for line in trigger_lines
            if line.startswith("  ")
            and not line.startswith("    ")
            and line.strip().endswith(":")
        }
        self.assertEqual(triggers, {"workflow_dispatch"})

    def test_rejects_http_write_methods_targeting_order_endpoints(self) -> None:
        probes = {
            "crates/probe/src/http.rs": (
                'reqwest::Client::new().post("https://clob.polymarket.com/order")'
                ".send().await?;\n"
            ),
            "scripts/submit.sh": (
                "curl -X POST https://clob.polymarket.com/order\n"
            ),
            ".github/workflows/delete.yml": (
                "run: curl --request DELETE https://clob.polymarket.com/order/42\n"
            ),
            "demo/delete.js": 'client.delete("/order/42");\n',
        }
        for relative_path, body in probes.items():
            write_public_file(self.root, relative_path, body)

        findings = scan_repository(self.root)

        self.assertEqual(
            {
                finding.path
                for finding in findings
                if finding.reason == "external order-submission capability"
            },
            set(probes),
        )

    def test_rejects_multiline_http_order_request_expressions(self) -> None:
        write_public_file(
            self.root,
            "crates/probe/src/multiline.rs",
            "let response = client\n"
            "    .post(\n"
            '        "https://clob.polymarket.com/order",\n'
            "    )\n"
            "    .send()\n"
            "    .await?;\n",
        )
        write_public_file(
            self.root,
            "scripts/multiline.sh",
            "curl -X POST \\\n"
            "  https://clob.polymarket.com/orders/42\n",
        )

        findings = scan_repository(self.root)

        self.assertEqual(
            {
                (finding.path, finding.line, finding.reason)
                for finding in findings
            },
            {
                (
                    "crates/probe/src/multiline.rs",
                    3,
                    "external order-submission capability",
                ),
                (
                    "scripts/multiline.sh",
                    2,
                    "external order-submission capability",
                ),
            },
        )

    def test_rejects_bounded_requests_regardless_of_method_endpoint_order(self) -> None:
        probes = {
            "crates/probe/src/generic_request.rs": (
                "let response = client\n"
                "    .request(\n"
                "        Method::POST,\n"
                '        "https://clob.polymarket.com/order",\n'
                "    )\n"
                "    .await?;\n"
            ),
            "demo/fetch.js": (
                "fetch(\n"
                '  "/order",\n'
                '  { method: "POST" },\n'
                ");\n"
            ),
            "scripts/compact-curl.sh": (
                "curl -XPOST \\\n"
                "  https://clob.polymarket.com/order\n"
            ),
            ".github/workflows/request-equals.yml": (
                "run: curl --request=POST \\\n"
                "  https://clob.polymarket.com/orders/1\n"
            ),
        }
        for relative_path, body in probes.items():
            write_public_file(self.root, relative_path, body)

        findings = scan_repository(self.root)

        self.assertEqual(
            {
                (finding.path, finding.line, finding.reason)
                for finding in findings
            },
            {
                (
                    "crates/probe/src/generic_request.rs",
                    4,
                    "external order-submission capability",
                ),
                (
                    "demo/fetch.js",
                    2,
                    "external order-submission capability",
                ),
                (
                    "scripts/compact-curl.sh",
                    2,
                    "external order-submission capability",
                ),
                (
                    ".github/workflows/request-equals.yml",
                    2,
                    "external order-submission capability",
                ),
            },
        )

    def test_allows_public_data_gets_and_exact_negative_documentation(self) -> None:
        write_public_file(
            self.root,
            "crates/probe/src/public_data.rs",
            'client.get("https://clob.polymarket.com/orderbook").send().await?;\n',
        )
        write_public_file(
            self.root,
            "scripts/public_data.sh",
            "curl https://clob.polymarket.com/orders\n",
        )
        write_public_file(
            self.root,
            "demo/separate-fetch.js",
            'fetch("/order");\nconst method = "POST";\n',
        )
        write_public_file(
            self.root,
            "scripts/separate-curl.sh",
            "curl https://clob.polymarket.com/order\nprintf POST\n",
        )
        write_public_file(
            self.root,
            "crates/probe/src/separate_requests.rs",
            'client.request(Method::GET, "/order");\nclient.post("/markets");\n',
        )
        write_public_file(
            self.root,
            "docs/publication-boundary.md",
            "`POST /order` is forbidden by the publication boundary.\n",
        )

        write_public_file(
            self.root,
            "docs/unapproved-negative-example.md",
            "`POST /order` is forbidden by the publication boundary.\n",
        )

        findings = scan_repository(self.root)

        self.assertEqual(
            [
                (finding.path, finding.line, finding.reason)
                for finding in findings
            ],
            [
                (
                    "docs/unapproved-negative-example.md",
                    1,
                    "external order-submission capability",
                )
            ],
        )

    def test_scans_every_declared_public_surface(self) -> None:
        public_files = [
            "scripts/probe.py",
            ".github/workflows/probe.yml",
            "docs/probe.md",
            "demo/probe.js",
            "fixtures/probe.json",
            "Cargo.toml",
            "Cargo.lock",
            "Dockerfile",
            ".env.example",
            "README.md",
            "SECURITY.md",
        ]
        for relative_path in public_files:
            write_public_file(self.root, relative_path, "client.cancel_order(order)\n")

        findings = scan_repository(self.root)

        self.assertEqual({finding.path for finding in findings}, set(public_files))
        self.assertTrue(
            all(
                finding.reason == "external order-submission capability"
                for finding in findings
            )
        )

    def test_ignores_non_public_and_generated_trees(self) -> None:
        for relative_path in [
            "notes/private.md",
            ".git/config",
            "target/generated.rs",
            "node_modules/package/index.js",
            "scripts/__pycache__/generated.py",
        ]:
            write_public_file(self.root, relative_path, "client.submit_order(order)\n")

        self.assertEqual(scan_repository(self.root), [])


if __name__ == "__main__":
    unittest.main()
