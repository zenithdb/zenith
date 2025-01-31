#! /usr/bin/env node

//
// The script parses Allure reports and posts a comment with a summary of the test results to the PR or to the latest commit in the branch.
//
// The comment is updated on each run with the latest results.
//
// It is designed to be used with actions/github-script from GitHub Workflows:
// - uses: actions/github-script@v6
//   with:
//     script: |
//       const script = require("./scripts/comment-test-report.js")
//       await script({
//         github,
//         context,
//         fetch,
//         report: {
//           reportUrl: "...",
//           reportJsonUrl: "...",
//         },
//         coverage: {
//           coverageUrl: "...",
//           summaryJsonUrl: "...",
//         }
//       })
//

// Equivalent of Python's defaultdict.
//
// const dm = new DefaultMap(() => new DefaultMap(() => []))
// dm["firstKey"]["secondKey"].push("value")
//
class DefaultMap extends Map {
    constructor(getDefaultValue) {
        return new Proxy({}, {
            get: (target, name) => name in target ? target[name] : (target[name] = getDefaultValue(name))
        })
    }
}

const parseReportJson = async ({ reportJsonUrl, fetch }) => {
    const suites = await (await fetch(reportJsonUrl)).json()

    // Allure distinguishes "failed" (with an assertion error) and "broken" (with any other error) tests.
    // For this report it's ok to treat them in the same way (as failed).
    const failedTests = new DefaultMap(() => new DefaultMap(() => []))
    const passedTests = new DefaultMap(() => new DefaultMap(() => []))
    const skippedTests = new DefaultMap(() => new DefaultMap(() => []))
    const retriedTests = new DefaultMap(() => new DefaultMap(() => []))
    const flakyTests = new DefaultMap(() => new DefaultMap(() => []))

    let failedTestsCount = 0
    let passedTestsCount = 0
    let skippedTestsCount = 0
    let flakyTestsCount = 0

    const pgVersions = new Set()

    for (const parentSuite of suites.children) {
        for (const suite of parentSuite.children) {
            for (const test of suite.children) {
                let buildType, pgVersion
                const match = test.name.match(/[\[-](?<buildType>debug|release)-pg(?<pgVersion>\d+)[-\]]/)?.groups
                if (match) {
                    ({ buildType, pgVersion } = match)
                } else {
                    // It's ok, we embed BUILD_TYPE and Postgres Version into the test name only for regress suite and do not for other suites (like performance).
                    console.info(`Cannot get BUILD_TYPE and Postgres Version from test name: "${test.name}", defaulting to "release" and "14"`)

                    buildType = "release"
                    pgVersion = "16"
                }

                pgVersions.add(pgVersion)

                // We use `arch` as it is returned by GitHub Actions
                //  (RUNNER_ARCH env var): X86, X64, ARM, or ARM64
                // Ref https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/store-information-in-variables#default-environment-variables
                let arch = ""
                if (test.parameters.includes("'X64'")) {
                    arch = "x86-64"
                } else if (test.parameters.includes("'ARM64'")) {
                    arch = "arm64"
                } else {
                    arch = "unknown"
                }
                let lfcState = ""
                if (test.parameters.includes("'with-lfc'")) {
                    lfcState = "with-lfc"
                } else {
                    lfcState = "without-lfc"
                }

                // Removing build type and PostgreSQL version from the test name to make it shorter
                const testName = test.name.replace(new RegExp(`${buildType}-pg${pgVersion}-?`), "").replace("[]", "")
                test.pytestName = `${parentSuite.name.replace(".", "/")}/${suite.name}.py::${testName}`
                test.pgVersion = pgVersion
                test.buildType = buildType
                test.arch = arch
                test.lfcState = lfcState

                if (test.status === "passed") {
                    passedTests[pgVersion][testName].push(test)
                    passedTestsCount += 1
                } else if (test.status === "failed" || test.status === "broken") {
                    failedTests[pgVersion][testName].push(test)
                    failedTestsCount += 1
                } else if (test.status === "skipped") {
                    skippedTests[pgVersion][testName].push(test)
                    skippedTestsCount += 1
                }

                if (test.retriesCount > 0) {
                    retriedTests[pgVersion][testName].push(test)

                    if (test.retriesStatusChange) {
                        flakyTests[pgVersion][testName].push(test)
                        flakyTestsCount += 1
                    }
                }
            }
        }
    }

    return {
        failedTests,
        failedTestsCount,
        passedTests,
        passedTestsCount,
        skippedTests,
        skippedTestsCount,
        flakyTests,
        flakyTestsCount,
        retriedTests,
        pgVersions,
    }
}

const reportSummary = async (params) => {
    const {
        failedTests,
        failedTestsCount,
        passedTests,
        passedTestsCount,
        skippedTests,
        skippedTestsCount,
        flakyTests,
        flakyTestsCount,
        retriedTests,
        pgVersions,
        reportUrl,
    } = params

    let summary = ""

    const totalTestsCount = failedTestsCount + passedTestsCount + skippedTestsCount
    summary += `### ${totalTestsCount} tests run: ${passedTestsCount} passed, ${failedTestsCount} failed, ${skippedTestsCount} skipped ([full report](${reportUrl}))\n___\n`

    // Print test resuls from the newest to the oldest Postgres version for release and debug builds.
    for (const pgVersion of Array.from(pgVersions).sort().reverse()) {
        if (Object.keys(failedTests[pgVersion]).length > 0) {
            summary += `#### Failures on Postgres ${pgVersion}\n\n`
            for (const [testName, tests] of Object.entries(failedTests[pgVersion])) {
                const links = []
                for (const test of tests) {
                    const allureLink = `${reportUrl}#suites/${test.parentUid}/${test.uid}`
                    links.push(`[${test.buildType}-${test.arch}-${test.lfcState}](${allureLink})`)
                }
                summary += `- \`${testName}\`: ${links.join(", ")}\n`
            }
        }
    }

    if (failedTestsCount > 0) {
        const testsToRerun = []
        for (const pgVersion of Object.keys(failedTests)) {
            for (const testName of Object.keys(failedTests[pgVersion])) {
                testsToRerun.push(...failedTests[pgVersion][testName].map(test => test.name))
            }
        }
        const command = `scripts/pytest -vv -n $(nproc) -k "${testsToRerun.join(" or ")}"`

        summary += "```\n"
        summary += `# Run all failed tests locally:\n`
        summary += `${command}\n`
        summary += "```\n"
    }

    if (flakyTestsCount > 0) {
        summary += `<details>\n<summary>Flaky tests (${flakyTestsCount})</summary>\n\n`
        for (const pgVersion of Array.from(pgVersions).sort().reverse()) {
            if (Object.keys(flakyTests[pgVersion]).length > 0) {
                summary += `#### Postgres ${pgVersion}\n\n`
                for (const [testName, tests] of Object.entries(flakyTests[pgVersion])) {
                    const links = []
                    for (const test of tests) {
                        const allureLink = `${reportUrl}#suites/${test.parentUid}/${test.uid}/retries`
                        links.push(`[${test.buildType}-${test.arch}-${test.lfcState}](${allureLink})`)
                    }
                    summary += `- \`${testName}\`: ${links.join(", ")}\n`
                }
            }
        }
        summary += "\n</details>\n"
    }

    return summary
}

const parseCoverageSummary = async ({ summaryJsonUrl, coverageUrl, fetch }) => {
    let summary = `\n### Code coverage ([full report](${coverageUrl}))\n`

    const coverage = await (await fetch(summaryJsonUrl)).json()
    for (const covType of Object.keys(coverage).sort()) {
        if (!coverage.hasOwnProperty(covType)) {
            continue
        }

        summary += `- \`${covType}s\`: \`${coverage[covType]["_summary"]}\`\n`
    }

    summary += `\n___\n`

    return summary
}

module.exports = async ({ github, context, fetch, report, coverage }) => {
    // If we run the script in the PR or in the branch (main/release/...)
    const isPullRequest = !!context.payload.pull_request
    // Which PR to comment (for ci-run/pr-* it will comment the parent PR, not the ci-run/pr-* PR)
    let prToComment
    if (isPullRequest) {
        const branchName = context.payload.pull_request.head.ref.replace(/^refs\/heads\//, "")
        const match = branchName.match(/ci-run\/pr-(?<prNumber>\d+)/)?.groups
        if (match) {
            ({ prNumber } = match)
            prToComment = parseInt(prNumber, 10)
        } else {
            prToComment = context.payload.number
        }
    }
    // Marker to find the comment in the subsequent runs
    const startMarker = `<!--AUTOMATIC COMMENT START #${prToComment}-->`
    // Latest commit in PR or in the branch
    const commitSha = isPullRequest ? context.payload.pull_request.head.sha : context.sha
    // Let users know that the comment is updated automatically
    const autoupdateNotice = `<div align="right"><sub>The comment gets automatically updated with the latest test results<br>${commitSha} at ${new Date().toISOString()} :recycle:</sub></div>`
    // GitHub bot id taken from (https://api.github.com/users/github-actions[bot])
    const githubActionsBotId = 41898282
    // Commend body itself
    let commentBody = `${startMarker}\n`

    // Common parameters for GitHub API requests
    const ownerRepoParams = {
        owner: context.repo.owner,
        repo: context.repo.repo,
    }

    const {reportUrl, reportJsonUrl} = report
    if (reportUrl && reportJsonUrl) {
        try {
            const parsed = await parseReportJson({ reportJsonUrl, fetch })
            commentBody += await reportSummary({ ...parsed, reportUrl })
        } catch (error) {
            commentBody += `### [full report](${reportUrl})\n___\n`
            commentBody += `#### Failed to create a summary for the test run: \n`
            commentBody += "```\n"
            commentBody += `${error.stack}\n`
            commentBody += "```\n"
            commentBody += "\nTo reproduce and debug the error locally run:\n"
            commentBody += "```\n"
            commentBody += `scripts/comment-test-report.js ${reportJsonUrl}`
            commentBody += "\n```\n"
        }
    } else {
        commentBody += `#### No tests were run or test report is not available\n`
    }

    const { coverageUrl, summaryJsonUrl } = coverage
    if (coverageUrl && summaryJsonUrl) {
        try {
            commentBody += await parseCoverageSummary({ summaryJsonUrl, coverageUrl, fetch })
        } catch (error) {
            commentBody += `### [full report](${coverageUrl})\n___\n`
            commentBody += `#### Failed to create a coverage summary for the test run: \n`
            commentBody += "```\n"
            commentBody += `${error.stack}\n`
            commentBody += "```\n"
        }
    } else {
        commentBody += `\n#### Test coverage report is not available\n`
    }

    commentBody += autoupdateNotice

    let createCommentFn, listCommentsFn, updateCommentFn, issueNumberOrSha
    if (isPullRequest) {
        createCommentFn  = github.rest.issues.createComment
        listCommentsFn   = github.rest.issues.listComments
        updateCommentFn  = github.rest.issues.updateComment
        issueNumberOrSha = {
            issue_number: prToComment,
        }
    } else {
        updateCommentFn  = github.rest.repos.updateCommitComment
        listCommentsFn   = github.rest.repos.listCommentsForCommit
        createCommentFn  = github.rest.repos.createCommitComment
        issueNumberOrSha = {
            commit_sha: commitSha,
        }
    }

    const { data: comments } = await listCommentsFn({
        ...issueNumberOrSha,
        ...ownerRepoParams,
    })

    const comment = comments.find(comment => comment.user.id === githubActionsBotId && comment.body.startsWith(startMarker))
    if (comment) {
        await updateCommentFn({
            comment_id: comment.id,
            body: commentBody,
            ...ownerRepoParams,
        })
    } else {
        await createCommentFn({
            body: commentBody,
            ...issueNumberOrSha,
            ...ownerRepoParams,
        })
    }
}

// Equivalent of Python's `if __name__ == "__main__":`
// https://nodejs.org/docs/latest/api/modules.html#accessing-the-main-module
if (require.main === module) {
    // Poor man's argument parsing: we expect the third argument is a JSON URL (0: node binary, 1: this script, 2: JSON url)
    if (process.argv.length !== 3) {
        console.error(`Unexpected number of arguments\nUsage: node ${process.argv[1]} <jsonUrl>`)
        process.exit(1)
    }
    const jsonUrl = process.argv[2]

    try {
        new URL(jsonUrl)
    } catch (error) {
        console.error(`Invalid URL: ${jsonUrl}\nUsage: node ${process.argv[1]} <jsonUrl>`)
        process.exit(1)
    }

    const htmlUrl = jsonUrl.replace("/data/suites.json", "/index.html")

    const githubMock = {
        rest: {
            issues: {
                createComment: console.log,
                listComments: async () => ({ data: [] }),
                updateComment: console.log
            },
            repos: {
                createCommitComment: console.log,
                listCommentsForCommit: async () => ({ data: [] }),
                updateCommitComment: console.log
            }
        }
    }

    const contextMock = {
        repo: {
            owner: 'testOwner',
            repo: 'testRepo'
        },
        payload: {
            number: 42,
            pull_request: null,
        },
        sha: '0000000000000000000000000000000000000000',
    }

    module.exports({
        github: githubMock,
        context: contextMock,
        fetch: fetch,
        report: {
            reportUrl: htmlUrl,
            reportJsonUrl: jsonUrl,
        }
    })
}
