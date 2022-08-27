<?php

/**
 * Qubus\Expressive
 *
 * @link       https://github.com/QubusPHP/expressive
 * @copyright  2022
 * @author     Joshua Parker <joshua@joshuaparker.dev>
 * @license    https://opensource.org/licenses/mit-license.php MIT License
 *
 * @since      0.1.0
 */

declare(strict_types=1);

namespace Qubus\Tests\Expressive\DataMapper;

use Qubus\Expressive\DataMapper\Entity;
use Qubus\Expressive\DataMapper\Property;
use Qubus\Expressive\DataMapper\SerializableEntity;

#[Entity('qub_user')]
class User extends SerializableEntity
{
    #[Property('user_id')]
    public int|string $id;

    #[Property('user_login')]
    public int|string $login;

    #[Property('user_fname')]
    public string $fname;

    #[Property('user_lname')]
    public string $lname;

    #[Property('user_email')]
    public string $email;

    #[Property('user_pass')]
    public string $pass;

    #[Property('user_registered')]
    public string $registered;
}
