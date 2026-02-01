<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration;

use ArrayAccess;
use Qubus\Expressive\Migration\Adapter\MigrationAdapter;
use Symfony\Component\Console\Output\OutputInterface;

class Migrator
{
    protected ?ArrayAccess $objectmap = null;

    protected ?MigrationAdapter $adapter = null;

    protected ?OutputInterface $output = null;

    /**
     * Constructor
     *
     * @param MigrationAdapter $adapter
     * @param ArrayAccess $objectmap
     * @param OutputInterface $output
     */
    public function __construct(MigrationAdapter $adapter, ArrayAccess $objectmap, OutputInterface $output)
    {
        $this->objectmap  = $objectmap;
        $this->adapter    = $adapter;
        $this->output     = $output;
    }

    /**
     * Run the up method on a migration
     *
     * @param Migration $migration
     * @return void
     */
    public function up(Migration $migration): void
    {
        $this->run(migration: $migration, direction: 'up');
    }

    /**
     * Run the down method on a migration
     *
     * @param Migration $migration
     * @return void
     */
    public function down(Migration $migration): void
    {
        $this->run(migration: $migration, direction: 'down');
    }

    /**
     * Run a migration in a particular direction
     *
     * @param Migration $migration
     * @param string $direction
     * @return void
     */
    protected function run(Migration $migration, string $direction = 'up'): void
    {
        $direction = ($direction == 'down' ? 'down' : 'up');
        $this->getOutput()?->writeln(
            messages: sprintf(
                ' == <info>' .
                $migration->getVersion() . ' ' .
                $migration->getName() . '</info> ' .
                '<comment>' .
                ($direction == 'up' ? 'migrating' : 'reverting') .
                '</comment>'
            )
        );
        $start = microtime(as_float: true);
        $migration->setObjectMap($this->getObjectMap());
        $migration->init();
        $migration->{$direction}();
        $this->getAdapter()->{$direction}($migration);
        $end = microtime(as_float: true);
        $this->getOutput()?->writeln(
            messages: sprintf(
                ' == <info>' .
                $migration->getVersion() . ' ' .
                $migration->getName() . '</info> ' .
                '<comment>' .
                ($direction == 'up' ? 'migrated ' : 'reverted ') .
                sprintf("%.4fs", $end - $start) .
                '</comment>'
            )
        );
    }

    /**
     * Get ObjectMap.
     *
     * @return ArrayAccess
     */
    public function getObjectMap(): ArrayAccess
    {
        return $this->objectmap;
    }

    /**
     * Set ObjectMap.
     *
     * @param ArrayAccess $objectmap
     * @return self
     */
    public function setObjectMap(ArrayAccess $objectmap): self
    {
        $this->objectmap = $objectmap;
        return $this;
    }

    /**
     * Get Adapter
     *
     * @return MigrationAdapter|null
     */
    public function getAdapter(): ?MigrationAdapter
    {
        return $this->adapter;
    }

    /**
     * Set Adapter
     *
     * @param MigrationAdapter $adapter
     * @return self
     */
    public function setAdapter(MigrationAdapter $adapter): self
    {
        $this->adapter = $adapter;
        return $this;
    }

    /**
     * Get Output
     *
     * @return OutputInterface|null
     */
    public function getOutput(): ?OutputInterface
    {
        return $this->output;
    }

    /**
     * Set Output
     *
     * @param OutputInterface $output
     * @return self
     */
    public function setOutput(OutputInterface $output): self
    {
        $this->output = $output;
        return $this;
    }
}
