import * as Docker from 'dockerode';

// Jest global teardown to stop and remove the container
export default async function teardown(): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  const containerIds = ((globalThis as any).__TEST_DOCKER_CONTAINER_IDS as string[]) ?? [];
  for (const containerId of containerIds) {
    console.log(`Stopping and removing container ${containerId}...`);
    const docker = new Docker();
    const container = docker.getContainer(containerId);
    const info = await container
      .inspect()
      .then(i => i.Image)
      .catch(() => '?');
    await container.stop().catch(error => {
      console.error(`Failed to stop container ${containerId}: ${error}`);
    });
    await container.remove({ v: true });
    console.log(`Test docker container ${info} ${containerId} stopped and removed`);
  }
}
